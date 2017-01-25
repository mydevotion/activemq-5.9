/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.TransportLoggerSupport;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.*;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 */
public class TcpTransportFactory extends TransportFactory {
    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportFactory.class);

    public TransportServer doBind(final URI location) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            ServerSocketFactory serverSocketFactory = createServerSocketFactory();
            TcpTransportServer server = createTcpTransportServer(location, serverSocketFactory);
            server.setWireFormatFactory(createWireFormatFactory(options));
            IntrospectionSupport.setProperties(server, options);
            Map<String, Object> transportOptions = IntrospectionSupport.extractProperties(options, "transport.");
            server.setTransportOption(transportOptions);
            server.bind();

            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransportServer.
     *
     * @param location
     * @param serverSocketFactory
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TcpTransportServer createTcpTransportServer(final URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory);
    }

    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {

        TcpTransport tcpTransport = (TcpTransport) transport.narrow(TcpTransport.class);
        // 这个地方会把参数设置到tcpTransport上
        IntrospectionSupport.setProperties(tcpTransport, options);

        // 或者是通过这个地方在key上添加"socket."前缀配置TcpTransport，反正两种方式都是设置到 tcpTransport的Socket上
        Map<String, Object> socketOptions = IntrospectionSupport.extractProperties(options, "socket.");
        tcpTransport.setSocketOptions(socketOptions);

        if (tcpTransport.isTrace()) {
            try {
                transport = TransportLoggerSupport.createTransportLogger(transport, tcpTransport.getLogWriterName(), tcpTransport.isDynamicManagement(), tcpTransport.isStartLogging(), tcpTransport.getJmxPort());
            } catch (Throwable e) {
                LOG.error("Could not create TransportLogger object for: " + tcpTransport.getLogWriterName() + ", reason: " + e, e);
            }
        }

        boolean useInactivityMonitor = "true".equals(getOption(options, "useInactivityMonitor", "true"));
        if (useInactivityMonitor && isUseInactivityMonitor(transport)) {
            transport = createInactivityMonitor(transport, format);
            IntrospectionSupport.setProperties(transport, options);
        }

        // Only need the WireFormatNegotiator if using openwire
        if (format instanceof OpenWireFormat) {
            transport = new WireFormatNegotiator(transport, (OpenWireFormat) format, tcpTransport.getMinmumWireFormatVersion());
        }

        return super.compositeConfigure(transport, format, options);
    }


    /**
     * Returns true if the inactivity monitor should be used on the transport
     */
    protected boolean isUseInactivityMonitor(Transport transport) {
        return true;
    }

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        URI localLocation = null;
        // path取的是URI里面，第一个"/"之后的部分，但是不包括"?"之后的部分
        String path = location.getPath();
        // see if the path is a local URI location
        if (path != null && path.length() > 0) {
            int localPortIndex = path.indexOf(':');
            try {
                Integer.parseInt(path.substring(localPortIndex + 1, path.length()));
                String localString = location.getScheme() + ":/" + path;
                localLocation = new URI(localString);
            } catch (Exception e) {
                LOG.warn("path isn't a valid local location for TcpTransport to use", e.getMessage());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failure detail", e);
                }
            }
        }
        // 如果location形如"tcp://192.168.3:61616/127.0.0.1:8080"，则localLocation为"tcp://127.0.0.1:8080"
        SocketFactory socketFactory = createSocketFactory();
        return createTcpTransport(wf, socketFactory, location, localLocation);
    }

    /**
     * new 一个TcpTransport返回
     * Allows subclasses of TcpTransportFactory to provide a create custom
     * TcpTransport intances.
     *
     * @param location
     * @param wf
     * @param socketFactory
     * @param localLocation
     * @return
     * @throws UnknownHostException
     * @throws IOException
     */
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new TcpTransport(wf, socketFactory, location, localLocation);
    }

    protected ServerSocketFactory createServerSocketFactory() throws IOException {
        return ServerSocketFactory.getDefault();
    }

    protected SocketFactory createSocketFactory() throws IOException {
        return SocketFactory.getDefault();
    }

    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
        return new InactivityMonitor(transport, format);
    }
}
