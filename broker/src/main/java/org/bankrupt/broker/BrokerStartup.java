package org.bankrupt.broker;

import org.bankrupt.broker.config.BrokerConfig;


public class BrokerStartup {

    public static void main(String[] args) {
        BrokerConfig brokerConfig = new BrokerConfig();
        BrokerController brokerController = new BrokerController(brokerConfig);
        brokerController.start();
        //钩子函数,关闭资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> brokerController.close()));
    }

}
