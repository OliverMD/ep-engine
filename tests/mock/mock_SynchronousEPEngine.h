//
// Created by Oliver Downard on 09/02/2017.
//
#pragma once

#include <ep_engine.h>
#include <ep_bucket.h>

class MockEPStore;

/* A class which subclasses the real EPEngine. Its main purpose is to allow
 * us to construct and setup an EPStore without starting all the various
 * background tasks which are normally started by EPEngine as part of creating
 * EPStore (in the initialize() method).
 *
 * The net result is a (mostly) synchronous environment - while the
 * ExecutorPool's threads exist, none of the normally-created background Tasks
 * should be running. Note however that /if/ any new tasks are created, they
 * will be scheduled on the ExecutorPools' threads asynchronously.
 */
class SynchronousEPEngine : public EventuallyPersistentEngine {
public:
    SynchronousEPEngine(const std::string& extra_config);

    void setEPStore(KVBucket* store);

    /* Allow us to call normally protected methods */

    ENGINE_ERROR_CODE public_doTapVbTakeoverStats(const void *cookie,
                                                  ADD_STAT add_stat,
                                                  std::string& key,
                                                  uint16_t vbid) {
        return doTapVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    ENGINE_ERROR_CODE public_doDcpVbTakeoverStats(const void *cookie,
                                                  ADD_STAT add_stat,
                                                  std::string& key,
                                                  uint16_t vbid) {
        return doDcpVbTakeoverStats(cookie, add_stat, key, vbid);
    }

    void public_initializeEngineCallbacks() {
        return initializeEngineCallbacks();
    }

    /*
     * Initialize the connmap objects, which creates tasks
     * so must be done after executorpool is created
     */
    void initializeConnmaps();
};

/* Subclass of EPStore to expose normally non-public members for test
 * purposes.
 */
class MockEPStore : public EPBucket {
public:
    MockEPStore(EventuallyPersistentEngine &theEngine);

    virtual ~MockEPStore() {}

    VBucketMap& getVbMap();

    void public_stopWarmup() {
        stopWarmup();
    }

    GetValue public_getInternal(const StoredDocKey& key, uint16_t vbucket,
                                const void* cookie, vbucket_state_t allowedState,
                                get_options_t options) {
        return getInternal(key, vbucket, cookie, allowedState, options);
    }
};
