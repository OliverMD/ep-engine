//
// Created by Oliver Downard on 06/02/2017.
//

#include <benchmark/benchmark.h>
#include <access_scanner.h>
#include <mock/mock_SynchronousEPEngine.h>
#include "dcp/dcpconnmap.h"
#include <tests/module_tests/makestoreddockey.h>
#include <programs/engine_testapp/mock_server.h>
#include <ep_test_apis.h>
#include <memcached/engine_error.h>

class EngineFixture : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State& state) override {

        HashTable::setDefaultNumLocks(state.range(2));

        std::string config = "dbname=benchmark-test;"
                "alog_resident_ratio_threshold=100;";

        engine.reset(new SynchronousEPEngine(config));
        ObjectRegistry::onSwitchThread(engine.get());

        store = new MockEPStore(*engine);
        engine->setEPStore(store);

        engine->public_initializeEngineCallbacks();
        initialize_time_functions(get_mock_server_api()->core);
        cookie = create_mock_cookie();

    }

    void TearDown(const benchmark::State& state) override {
        destroy_mock_cookie(cookie);
        destroy_mock_event_callbacks();
        engine->getDcpConnMap().manageConnections();
        engine.reset();
        ObjectRegistry::onSwitchThread(nullptr);
        ExecutorPool::shutdown();
    }

    Item make_item(uint16_t vbid, const StoredDocKey& key, const std::string&
    value){
        uint8_t ext_meta[EXT_META_LEN] = {PROTOCOL_BINARY_DATATYPE_JSON};
        Item item(key, /*flags*/0, /*exp*/0, value.c_str(), value.size(),
                  ext_meta, sizeof(ext_meta));
        item.setVBucketId(vbid);
        return item;
    }

    std::unique_ptr<SynchronousEPEngine> engine;
    MockEPStore* store;
    const void* cookie;
    const int vbid = 0;
};


BENCHMARK_DEFINE_F(EngineFixture, ConstantAccessScanner)(benchmark::State&
state){
    store->setVBucketState(0, vbucket_state_active, false);
    if(state.range(1) == 1) {
        state.SetLabel("AccessScanner");
        ExTask task = make_STRCPtr<AccessScanner>(*store, engine->getEpStats(),
                                                  0);
        ExecutorPool::get()->schedule(task, AUXIO_TASK_IDX);
    } else {
        state.SetLabel("Control");
    }
    std::string value(200, 'x');
    std::string value_1(200, 'y');

    std::vector<Item> items;
    //Prefill with 1000 items
    for(int i = 0; i < 1000; ++i) {
        auto item = make_item(vbid, makeStoredDocKey(std::to_string(i)), value);
        store->set(item, cookie);
    }
    for(int i = 0; i <state.range(0); ++i){
        auto item_1 = make_item(vbid, makeStoredDocKey(std::to_string(i)),
                                value_1);
        items.push_back(item_1);
    }
    while(state.KeepRunning()){
        for (auto item : items){
            store->set(item, cookie);
        }
    }
    state.SetItemsProcessed(state.range(0) * state.iterations());
}

static void CustomArguments(benchmark::internal::Benchmark* b){
    std::array<int, 7> lock_vals{{1, 2, 4, 8, 16, 32}};
    for (int i = 256; i <= 4096; i += 256){
        for(int j: lock_vals) {
            b->Args({i, 0, j});
            b->Args({i, 1, j});
        }
    }
}

BENCHMARK_REGISTER_F(EngineFixture, ConstantAccessScanner)->Apply
                (CustomArguments)->UseRealTime();

static char allow_no_stats_env[] = "ALLOW_NO_STATS_UPDATE=yeah";
int main(int argc, char** argv) {
    putenv(allow_no_stats_env);
    mock_init_alloc_hooks();
    init_mock_server(true);
    HashTable::setDefaultNumLocks(47);
    initialize_time_functions(get_mock_server_api()->core);
    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RunSpecifiedBenchmarks();
  }
