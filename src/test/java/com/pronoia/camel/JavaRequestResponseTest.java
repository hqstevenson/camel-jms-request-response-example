package com.pronoia.camel;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Rule;
import org.junit.Test;

public class JavaRequestResponseTest extends CamelTestSupport {
    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

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

    @EndpointInject( uri = "mock://result")
    MockEndpoint result;

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();

        ActiveMQComponent activeMQComponent = new ActiveMQComponent();
        activeMQComponent.setBrokerURL( broker.getVmURL() );

        registry.bind( "test-broker", activeMQComponent);

        return registry;
    }

    @Override
    protected RouteBuilder[] createRouteBuilders() throws Exception {
        RouteBuilder builders[] = new RouteBuilder[2];

        builders[1] = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("file://data?fileName=some-file.txt&noop=true")
                        .id( "file-consumer")
                        .log("Processing ${file:name}")
                        .process(requestProcessor)
                        .log("Generated ${body}")
                        .to(ExchangePattern.InOut, "test-broker://queue:testQueue")
                        .process(resultProcessor)
                        .to( "mock://result")
                ;
            }
        };

        builders[0] = new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from( "test-broker://queue:testQueue")
                        .id( "dummey-response-generator")
                        .setBody().constant( "Response Message")
                        .to(ExchangePattern.InOnly, "test-broker://queue:dummy-queue")
                ;

            }
        };

        return builders;
    }

    @Test
    public void testRoute() throws Exception {
        result.expectedBodiesReceived("We're good");

        assertMockEndpointsSatisfied(5, TimeUnit.SECONDS);
    }

}
