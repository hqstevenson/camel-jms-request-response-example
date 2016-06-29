package com.pronoia.camel;

import java.util.concurrent.TimeUnit;

import com.pronoia.camel.stub.jms.RequestReplyServer;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

public class JavaRequestResponseTest extends CamelTestSupport {
    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    final String requestResponseQueueName = "testQueue";
    final boolean useMessageIDAsCorrelationID = true;

    @EndpointInject(uri = "mock://result")
    MockEndpoint result;

    RequestReplyServer requestReplyServerStub = new RequestReplyServer();

    Processor requestProcessor = new Processor() {
        @Override
        public void process(Exchange exchange) throws Exception {
            String body = exchange.getIn().getBody(String.class);
            exchange.getIn().setBody("Request: " + body);

            return;
        }
    };

    Processor resultProcessor = new Processor() {
        @Override
        public void process(Exchange exchange) throws Exception {
            String body = exchange.getIn().getBody(String.class);
            if ( body.startsWith("Response") ) {
                exchange.getIn().setBody( "We're good");
            } else {
                exchange.getIn().setBody( "Bad response");
            }
            return;
        }
    };

    @Override
    protected void doPreSetup() throws Exception {
        requestReplyServerStub.setConnectionFactory(broker.createConnectionFactory());
        requestReplyServerStub.setRequestQueueName(requestResponseQueueName);
        requestReplyServerStub.setUseMessageIDAsCorrelationID(true);
        requestReplyServerStub.start();
        super.doPreSetup();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        requestReplyServerStub.interrupt();
        requestReplyServerStub.join(15000);
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();

        ActiveMQComponent activeMQComponent = new ActiveMQComponent();
        activeMQComponent.setBrokerURL( broker.getVmURL() );

        registry.bind( "test-broker", activeMQComponent);

        return registry;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        RouteBuilder builder = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file://data?fileName=some-file.txt&noop=true")
                        .id( "file-consumer")
                        .log("Processing ${file:name}")
                        .process(requestProcessor)
                        .log("Generated ${body}")
                        .toF("test-broker://queue:%s?exchangePattern=InOut&useMessageIDAsCorrelationID=%b",
                                requestResponseQueueName, useMessageIDAsCorrelationID)
                        .process(resultProcessor)
                        .to( "mock://result")
                ;
            }
        };

        return builder;
    }

    @Test
    public void testRoute() throws Exception {
        result.expectedBodiesReceived("We're good");

        assertMockEndpointsSatisfied(5, TimeUnit.SECONDS);
    }

}
