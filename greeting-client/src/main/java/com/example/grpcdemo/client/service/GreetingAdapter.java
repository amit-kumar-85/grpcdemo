package com.example.grpcdemo.client.service;

import com.example.grpcdemo.GreetingRequest;
import com.example.grpcdemo.GreetingResponse;
import com.example.grpcdemo.GreetingServiceGrpc;
import com.example.grpcdemo.StreamingGreetingResponse;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class GreetingAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(GreetingAdapter.class);

    @GrpcClient("greeting")
    private GreetingServiceGrpc.GreetingServiceBlockingStub greetingStub;

    @GrpcClient("greeting")
    private GreetingServiceGrpc.GreetingServiceStub greetingAsyncStub;

    public String greeting(String message) {
        GreetingResponse response = greetingStub
//                .withDeadlineAfter(100, TimeUnit.MILLISECONDS)
                .greeting(GreetingRequest.newBuilder().setMessage(message).build());
        LOG.info("Single RPC, response: {}", response.getMessage());
        return response.getMessage();
    }

    public String greetingSStream(String message) {
        LOG.info("Server Streaming RPC");
        GreetingRequest request = GreetingRequest.newBuilder().setMessage(message).build();
        Iterator<StreamingGreetingResponse> iterator = greetingStub
                .withDeadlineAfter(3000, TimeUnit.MILLISECONDS)
                .greetingSStream(request);
        List<String> msgs = new ArrayList<>();
        while (iterator.hasNext()) {
            StreamingGreetingResponse response = iterator.next();
            String msg = parseStreamingGreetingResponse(response);
            if(msg != null){
                msgs.add(msg);
            }
        }
        return String.join("-c-", msgs);
    }

    public String greetingCStream(String message) {
        LOG.info("Client Streaming RPC");
        List<String> msgs = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<GreetingResponse> responseObserver = new StreamObserver<GreetingResponse>() {
            @Override
            public void onNext(GreetingResponse greetingResponse) {
                LOG.info("message received");
                msgs.add(greetingResponse.getMessage());
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Error greeting response", throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOG.info("response onCompleted");
                latch.countDown();
            }
        };
        StreamObserver<GreetingRequest> requestObserver = greetingAsyncStub
                .withDeadlineAfter(3000, TimeUnit.MILLISECONDS)
                .greetingCStream(responseObserver);
        sendMultipleRequests(message, requestObserver);
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("await interrupted");
        }
        return String.join("-c-", msgs);
    }

    public String greetingBiStream(String message) {
        LOG.info("Bi-Directional GRPC");
        List<String> msgs = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<StreamingGreetingResponse> responseObserver = new StreamObserver<StreamingGreetingResponse>() {
            @Override
            public void onNext(StreamingGreetingResponse streamingGreetingResponse) {
                String msg = parseStreamingGreetingResponse(streamingGreetingResponse);
                if(msg != null){
                    msgs.add(msg);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Error greeting response", throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOG.info("response onCompleted");
                latch.countDown();
            }
        };
        StreamObserver<GreetingRequest> requestObserver = greetingAsyncStub
                .withDeadlineAfter(3000, TimeUnit.MILLISECONDS)
                .greetingBiDirectional(responseObserver);
        sendMultipleRequests(message, requestObserver);
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("await interrupted");
        }
        return String.join("-c-", msgs);
    }

    private void sendMultipleRequests(String message, StreamObserver<GreetingRequest> requestObserver) {
        if(message != null && message.length() > 0) {
            try {
                for (String msg : message.split(" ")) {
                    LOG.info("sending request, msg: {}", msg);
                    requestObserver.onNext(GreetingRequest.newBuilder().setMessage(msg).build());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("sleep interrupted");
                    }
                }
            } catch (RuntimeException e) {
                LOG.error("Error in sending requests", e);
                requestObserver.onError(e);
                throw e;
            }
        }
        LOG.info("completing the requests");
        requestObserver.onCompleted();
        LOG.info("returning from method");
    }

    private String parseStreamingGreetingResponse(StreamingGreetingResponse response) {
        LOG.info("got response, case: {}", response.getMessageCase());
        switch (response.getMessageCase()){
            case GREETINGRESPONSE:
                String msg = response.getGreetingResponse().getMessage();
                LOG.info("Greeting Response chunk: {}", msg);
                return msg;
            case STATUS:
                LOG.error("Error greeting response chunk, code: {}, msg: {}", response.getStatus().getCode(), response.getStatus().getMessage());
        }
        return null;
    }
}
