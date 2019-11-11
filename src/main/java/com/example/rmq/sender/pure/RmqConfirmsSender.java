package com.example.rmq.sender.pure;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RmqConfirmsSender {

    public static void main(String[] args) throws InterruptedException {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setPort(5672);
        cf.setVirtualHost("/");
        cf.setUsername("guest");
        cf.setPassword("guest");

        try (Connection connection = cf.newConnection("RmqConfirmsSender")) {
            log.info("Connection is created");

            try (Channel channel = connection.createChannel()) {
                log.info("Channel is created");
                channel.confirmSelect();

                channel.addConfirmListener(
                        (deliveryTag, multiple) -> log.info("Ack deliveryTag={}, multiple={}", deliveryTag, multiple),
                        (deliveryTag, multiple) -> log.info("Nack deliveryTag={}, multiple={}", deliveryTag, multiple));

                channel.basicPublish(
                        "amq.fanout",
                        "",
                        false,
                        new AMQP.BasicProperties.Builder()
                                .contentType("application/json")
                                .deliveryMode(1)
                                .headers(Collections.singletonMap("foo", "bar"))
                                .appId("testAppID")
                                .build(),
                        "{\"message\"=\"Hello world\"}".getBytes()
                );

                log.info("message is published");

                channel.waitForConfirms(5_000);

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
