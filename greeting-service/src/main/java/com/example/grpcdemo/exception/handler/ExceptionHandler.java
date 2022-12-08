package com.example.grpcdemo.exception.handler;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcAdvice
public class ExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandler.class);

    @GrpcExceptionHandler({StatusRuntimeException.class})
    public StatusRuntimeException handleRuntimeException(StatusRuntimeException e) {
        LOG.error("error response, message: {}", e.getMessage(), e);
        return e;
    }
}
