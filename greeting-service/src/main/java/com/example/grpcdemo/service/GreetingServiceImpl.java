package com.example.grpcdemo.service;

import com.example.grpcdemo.ErrorResponse;
import com.example.grpcdemo.GreetingRequest;
import com.example.grpcdemo.GreetingResponse;
import com.example.grpcdemo.GreetingServiceGrpc;
import com.example.grpcdemo.StreamingGreetingResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

// error reference : https://www.baeldung.com/grpcs-error-handling

@GrpcService
public class GreetingServiceImpl extends GreetingServiceGrpc.GreetingServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(GreetingServiceImpl.class);

    @Override
    public void greeting(GreetingRequest request, StreamObserver<GreetingResponse> responseObserver) {
        String message = request.getMessage();
        LOG.info("Received Message: {}", message);
        try {
            if (message.equalsIgnoreCase("error")) {
                throw new RuntimeException("errornous message");
            }
            GreetingResponse greetingResponse = GreetingResponse.newBuilder()
                    .setMessage("Received your " + message + ". Hello From Server. ")
                    .build();

            responseObserver.onNext(greetingResponse);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            LOG.error("runtime error response, within serviceImpl, error message: {}", e.getMessage(), e);
            Metadata.Key<ErrorResponse> errorResponseKey = ProtoUtils.keyForProto(ErrorResponse.getDefaultInstance());
            ErrorResponse errorResponse = ErrorResponse.newBuilder().setErrorMessage("Custom Error Message: " + message).build();

            Metadata metadata = new Metadata();
            metadata.put(errorResponseKey, errorResponse);
            responseObserver.onError(Status.INTERNAL.withDescription("Threw coz u said so").asRuntimeException(metadata));
        }
    }

    @Override
    public void greetingSStream(GreetingRequest request, StreamObserver<StreamingGreetingResponse> responseObserver) {
        if (request != null && request.getMessage() != null) {
            String[] arr = request.getMessage().split(" ");
            for (String str : arr) {
                if(Context.current().isCancelled()){
                    LOG.info("Cancelled by client");
                    responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
                    return;
                }
                LOG.info("sending response for chunk: {}", str);
                responseObserver.onNext(getStreamingGreetingResponse(str));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.error("sleep interrupted");
                }
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<GreetingRequest> greetingCStream(StreamObserver<GreetingResponse> responseObserver) {
        return new StreamObserver<GreetingRequest>() {
            private List<String> messages = new ArrayList<>();
            Date startTime = new Date(System.currentTimeMillis());

            @Override
            public void onNext(GreetingRequest greetingRequest) {
                if (greetingRequest.getMessage() != null && greetingRequest.getMessage().length() > 0) {
                    LOG.info("Got request for chunk: {}", greetingRequest.getMessage());
                    if(greetingRequest.getMessage().equalsIgnoreCase("error")){
                        responseObserver.onError(Status.INTERNAL.withDescription("Threw coz u said so").asRuntimeException());
                    } else {
                        messages.add(greetingRequest.getMessage());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Request Observer, onError", throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Request started at: {}, ended at: {}", startTime, new Date(System.currentTimeMillis()));
                responseObserver.onNext(GreetingResponse.newBuilder().setMessage(String.join("-s-", messages)).build());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<GreetingRequest> greetingBiDirectional(StreamObserver<StreamingGreetingResponse> responseObserver) {
        return new StreamObserver<GreetingRequest>() {
            @Override
            public void onNext(GreetingRequest greetingRequest) {
                LOG.info("Got request for chunk: {}", greetingRequest.getMessage());
                responseObserver.onNext(getStreamingGreetingResponse(greetingRequest.getMessage()));
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Request Observer, onError", throwable);
//                responseObserver.onNext(StreamingGreetingResponse.newBuilder().setGreetingResponse(GreetingResponse.newBuilder().setMessage("afterErr").build()).build());
            }

            @Override
            public void onCompleted() {
                LOG.info("Serving completed");
                responseObserver.onCompleted();
            }
        };
    }

    private StreamingGreetingResponse getStreamingGreetingResponse(String message) {
        if (message.equalsIgnoreCase("error")) {
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage("Error message found").build();
            return StreamingGreetingResponse.newBuilder().setStatus(status).build();
        } else {

            GreetingResponse greetingResponse = GreetingResponse.newBuilder().setMessage("Received chunk: " + message).build();
            return StreamingGreetingResponse.newBuilder().setGreetingResponse(greetingResponse).build();
        }
    }
}
