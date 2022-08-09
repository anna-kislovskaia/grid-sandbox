package com.grid.sandbox.service;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;

public class ClusterLifecycleListenerService implements MembershipListener, LifecycleListener {

    private Subject<ClusterStateChangeEvent> clusterEvents = BehaviorSubject.create();

    public Flowable<ClusterStateChangeEvent> getClusterStateFeed() {
        return clusterEvents.toFlowable(BackpressureStrategy.LATEST);
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        switch (event.getState()) {
            case STARTED:
                clusterEvents.onNext(new ClusterStateChangeEvent(ClusterStateChangeEvent.Type.CONNECTED));
                break;
            case SHUTDOWN:
                clusterEvents.onNext(new ClusterStateChangeEvent(ClusterStateChangeEvent.Type.DISCONNECTED));
                break;
        }
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        clusterEvents.onNext(new ClusterStateChangeEvent(ClusterStateChangeEvent.Type.MEMBER_ADDED));
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        clusterEvents.onNext(new ClusterStateChangeEvent(ClusterStateChangeEvent.Type.MEMBER_REMOVED));
    }
}
