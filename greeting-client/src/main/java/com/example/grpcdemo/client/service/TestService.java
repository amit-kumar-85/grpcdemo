package com.example.grpcdemo.client.service;

import com.example.grpcdemo.ErrorResponse;
import com.example.grpcdemo.client.enums.CallType;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TestService {

    private static final Logger LOG = LoggerFactory.getLogger(TestService.class);
    @Autowired
    private GreetingAdapter greetingAdapter;

    public String greeting(String message, CallType type) {
        try {
            switch (type){
                case SingleRpc:
                    return greetingAdapter.greeting(message);
                case ServerStreamingRpc:
                    return greetingAdapter.greetingSStream(message);
                case ClientStreamingRpc:
                    return greetingAdapter.greetingCStream(message);
                case BiDirectionalRpc:
                    return greetingAdapter.greetingBiStream(message);
                default:
                    throw new RuntimeException("invalid CallType");
            }

        } catch (StatusRuntimeException e) {
            LOG.error("error response, message: {}", e.getMessage(), e);

            Metadata metadata = Status.trailersFromThrowable(e);
            if (metadata != null) {
                ErrorResponse errorResponse = metadata.get(ProtoUtils.keyForProto(ErrorResponse.getDefaultInstance()));
                LOG.error("errorResponse: {}", errorResponse);
            }
            throw e;
        } catch (RuntimeException e) {
            LOG.error("error, message: {}", e.getMessage(), e);
            throw e;
        }
    }
}
