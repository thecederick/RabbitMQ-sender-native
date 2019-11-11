package com.example.rmq.sender.pure;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RmqTransactionalSender {

    public static void main(String[] args) throws InterruptedException {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setPort(5672);
        cf.setVirtualHost("/");
        cf.setUsername("guest");
        cf.setPassword("guest");

        try (Connection connection = cf.newConnection("RmqTransactionalSender")) {
            log.info("Connection is created");

            try (Channel channel = connection.createChannel()) {
                log.info("Channel is created");

                channel.txSelect();

                channel.basicPublish(
                        "amq.fanout",
                        "",
                        false,
                        new AMQP.BasicProperties.Builder()
                                .contentType("application/json")
                                .deliveryMode(1)
                                .headers(Collections.singletonMap("foo", "bar"))
                                .appId("testAppID")
                                .messageId("12345")
                                .timestamp(new Date())
                                .type("type")
                                .clusterId("cluster")
                                .userId("guest")
                                .correlationId("corr")
                                .build(),
                        "{\"message\"=\"Hello world\"}".getBytes()
                );
                log.info("message is published");


                channel.txCommit();
                //Thread.sleep(10_000);
                //channel.txRollback();

            } catch (IOException e) {
                log.error("Channel error", e);
            } finally {
                log.info("Channel is closed");
            }

        } catch (IOException | TimeoutException e) {
            log.error("Connection error", e);
        } finally {
            log.info("connection is closed");
        }

    }
}
