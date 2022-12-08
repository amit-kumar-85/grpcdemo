package com.example.grpcdemo.client.config;

import com.example.grpcdemo.GreetingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GreetingConfiguration {

//    @Bean
//    public GreetingServiceGrpc.GreetingServiceBlockingStub greetingBlockingStub() {
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 9090)
//                .usePlaintext()
//                .build();
//
//        return GreetingServiceGrpc.newBlockingStub(channel);
//    }
}
