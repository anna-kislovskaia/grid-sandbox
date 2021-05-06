package com.grid.sandbox.core.utils;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.subjects.BehaviorSubject;
import lombok.Getter;
import org.springframework.data.domain.Pageable;

import java.util.function.Predicate;

@Getter
public class ReportSubscription<T> {
    private final int subscriptionId;
    private final Scheduler scheduler;
    private final BehaviorSubject<Pageable> pageableSubject = BehaviorSubject.create();
    private final BehaviorSubject<Predicate<T>> userFilterSubject = BehaviorSubject.create();

    public ReportSubscription(int subscriptionId, Pageable request, Predicate<T> initial, Scheduler scheduler) {
        this.subscriptionId = subscriptionId;
        this.scheduler = scheduler;
        pageableSubject.onNext(request);
        userFilterSubject.onNext(initial);
    }

    public Flowable<Pageable> getRequestFeed() {
        return pageableSubject.toFlowable(BackpressureStrategy.LATEST);
    }

    public Flowable<Predicate<T>> getUserFilterFeed() {
        return userFilterSubject.toFlowable(BackpressureStrategy.LATEST);
    }
}
