package com.grid.sandbox.core.utils;

import com.grid.sandbox.core.model.BlotterViewport;
import com.grid.sandbox.core.model.UserBlotterSettings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.subjects.BehaviorSubject;
import lombok.Getter;
import org.springframework.data.domain.Sort;

@Getter
public class ReportSubscription {
    private final int subscriptionId;
    private final Scheduler scheduler;
    private final BehaviorSubject<BlotterViewport> viewportSubject = BehaviorSubject.create();
    private final BehaviorSubject<UserBlotterSettings> settingsSubject = BehaviorSubject.create();

    public ReportSubscription(int subscriptionId, BlotterViewport viewport, Sort initial, Scheduler scheduler) {
        this.subscriptionId = subscriptionId;
        this.scheduler = scheduler;
        viewportSubject.onNext(viewport);
        settingsSubject.onNext(new UserBlotterSettings(initial, ""));
    }

    public Flowable<BlotterViewport> getViewportFeed() {
        return viewportSubject.toFlowable(BackpressureStrategy.LATEST).distinctUntilChanged();
    }

    public Flowable<UserBlotterSettings> getUserSettingsFeed() {
        return settingsSubject.toFlowable(BackpressureStrategy.LATEST).distinctUntilChanged();
    }
}
