package com.grid.sandbox.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Aspect
@Component
public class LoggingAspect {

    @Pointcut("execution(* com.grid.sandbox.controller.*.*(..))")
    private void controllerCall() {}


    @Before("controllerCall()")
    public void logBefore(JoinPoint joinPoint) {
        Logger log = LogManager.getLogger(joinPoint.getTarget().getClass());
        log.info("{}({})",
                joinPoint.getSignature().getName(),
                joinPoint.getArgs() == null || joinPoint.getArgs().length == 0 ? "" : Arrays.toString(joinPoint.getArgs())
        );
    }

    @AfterReturning("controllerCall()")
    public void logAfterSuccess(JoinPoint joinPoint) {
        Logger log = LogManager.getLogger(joinPoint.getTarget().getClass());
        log.info("{} = ok",
                joinPoint.getSignature().getName()
        );
    }

    @AfterThrowing(pointcut = "controllerCall()", throwing  = "t")
    public void logAfterException(JoinPoint joinPoint, Throwable t) {
        Logger log = LogManager.getLogger(joinPoint.getTarget().getClass());
        log.error("{} = throws {}",
                joinPoint.getSignature().getName(),
                t.getClass().getName()
        );
    }}
