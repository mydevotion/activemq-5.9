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
package org.apache.activemq.transport;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public abstract class TransportFactory {

    /**
     * 去client下面的/services/org/apache/activemq/transport/找TransportFactory的定义
     */
    private static final FactoryFinder TRANSPORT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");
    private static final ConcurrentHashMap<String, TransportFactory> TRANSPORT_FACTORYS = new ConcurrentHashMap<String, TransportFactory>();

    private static final String WRITE_TIMEOUT_FILTER = "soWriteTimeout";
    private static final String THREAD_NAME_FILTER = "threadName";

    public abstract TransportServer doBind(URI location) throws IOException;

    public Transport doConnect(URI location, Executor ex) throws Exception {
        return doConnect(location);
    }

    public Transport doCompositeConnect(URI location, Executor ex) throws Exception {
        return doCompositeConnect(location);
    }

    /**
     * 根据传入参数location的scheme,去从缓存中获取TransportFactory，
     * 如果获取不到，则去"META-INF/services/org/apache/activemq/transport/"加载。
     * 然后通过TransportFactory的doConnect返回Transport
     * Creates a normal transport.
     *
     * @param location
     * @return the transport
     * @throws Exception
     */
    public static Transport connect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location);
    }

    /**
     * Creates a normal transport.
     *
     * @param location
     * @param ex
     * @return the transport
     * @throws Exception
     */
    public static Transport connect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doConnect(location, ex);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     *
     * @param location
     * @return the Transport
     * @throws Exception
     */
    public static Transport compositeConnect(URI location) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location);
    }

    /**
     * Creates a slimmed down transport that is more efficient so that it can be
     * used by composite transports like reliable and HA.
     *
     * @param location
     * @param ex
     * @return the Transport
     * @throws Exception
     */
    public static Transport compositeConnect(URI location, Executor ex) throws Exception {
        TransportFactory tf = findTransportFactory(location);
        return tf.doCompositeConnect(location, ex);
    }

    public static TransportServer bind(URI location) throws IOException {
        TransportFactory tf = findTransportFactory(location);
        return tf.doBind(location);
    }

    /**
     * 从传入的URI中获取参数配置
     *
     * @param location
     * @return
     * @throws Exception
     */
    public Transport doConnect(URI location) throws Exception {
        try {
            // 从URL中取出配置参数，初始化WireFormat和Transport
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            if (!options.containsKey("wireFormat.host")) {
                options.put("wireFormat.host", location.getHost());
            }
            // 将以"wireFormat."开头的属性设置到WireFormat属性里面的WireFormatInfo属性上
            WireFormat wf = createWireFormat(options);
            //
            Transport transport = createTransport(location, wf);
            Transport rc = configure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public Transport doCompositeConnect(URI location) throws Exception {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));
            WireFormat wf = createWireFormat(options);
            Transport transport = createTransport(location, wf);
            Transport rc = compositeConfigure(transport, wf, options);
            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid connect parameters: " + options);
            }
            return rc;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Allow registration of a transport factory without wiring via META-INF classes
     *
     * @param scheme
     * @param tf
     */
    public static void registerTransportFactory(String scheme, TransportFactory tf) {
        TRANSPORT_FACTORYS.put(scheme, tf);
    }

    /**
     * 创建一个新的transport，需要子类继承
     * Factory method to create a new transport
     *
     * @throws IOException
     * @throws UnknownHostException
     */
    protected Transport createTransport(URI location, WireFormat wf) throws MalformedURLException, UnknownHostException, IOException {
        throw new IOException("createTransport() method not implemented!");
    }

    /**
     * 根据传入的location的scheme去缓存中或者"/services/org/apache/activemq/transport"中加载TransportFactory
     *
     * @param location
     * @return
     * @throws IOException
     */
    public static TransportFactory findTransportFactory(URI location) throws IOException {
        // 取的URI的scheme,如"tcp://localhost:61616"，则返回"tcp"
        String scheme = location.getScheme();
        if (scheme == null) {
            throw new IOException("Transport not scheme specified: [" + location + "]");
        }
        // 去缓存中取
        TransportFactory tf = TRANSPORT_FACTORYS.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                // 如果缓存中不存在，就创建
                // 根据scheme去"/services/org/apache/activemq/transport"下面找配置，然后加载类
                tf = (TransportFactory) TRANSPORT_FACTORY_FINDER.newInstance(scheme);
                TRANSPORT_FACTORYS.put(scheme, tf);
            } catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }

    /**
     * 实现配置类在
     * "META-INF/services/org/apache/activemq/wireformat/"
     * 默认使用{@link org.apache.activemq.openwire.OpenWireFormatFactory}
     *
     * @param options
     * @return
     * @throws IOException
     */
    protected WireFormat createWireFormat(Map<String, String> options) throws IOException {
        WireFormatFactory factory = createWireFormatFactory(options);
        WireFormat format = factory.createWireFormat();
        return format;
    }

    /**
     * 生成WireFormatFactory，如果传入的参数中没有key为"wireFormat"的值，则默认采用"default"
     * 根据反射机制，将options的key有以"wireFormat."开头的属性设置到WireFormatFactory上
     *
     * @param options
     * @return
     * @throws IOException
     */
    protected WireFormatFactory createWireFormatFactory(Map<String, String> options) throws IOException {
        String wireFormat = (String) options.remove("wireFormat");
        if (wireFormat == null) {
            wireFormat = getDefaultWireFormatType();
        }

        try {
            WireFormatFactory wff = (WireFormatFactory) WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options, "wireFormat.");
            return wff;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }

    protected String getDefaultWireFormatType() {
        return "default";
    }

    /**
     * Fully configures and adds all need transport filters so that the
     * transport can be used by the JMS client.
     *
     * @param transport
     * @param wf
     * @param options
     * @return
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public Transport configure(Transport transport, WireFormat wf, Map options) throws Exception {
        transport = compositeConfigure(transport, wf, options);
        // 继续进行封装，加入syncOnCommand = false
        transport = new MutexTransport(transport);
        //继续封装
        transport = new ResponseCorrelator(transport);

        return transport;
    }

    /**
     * Fully configures and adds all need transport filters so that the
     * transport can be used by the ActiveMQ message broker. The main difference
     * between this and the configure() method is that the broker does not issue
     * requests to the client so the ResponseCorrelator is not needed.
     *
     * @param transport
     * @param format
     * @param options
     * @return
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        if (options.containsKey(THREAD_NAME_FILTER)) {
            transport = new ThreadNameFilter(transport);
        }
        transport = compositeConfigure(transport, format, options);
        // 对transport有进行了封装
        transport = new MutexTransport(transport);
        return transport;
    }

    /**
     * Similar to configure(...) but this avoid adding in the MutexTransport and
     * ResponseCorrelator transport layers so that the resulting transport can
     * more efficiently be used as part of a composite transport.
     *
     * @param transport
     * @param format
     * @param options
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        // 如果URI里面有配置参数"soWriteTimeout"，则对transport进行封装
        if (options.containsKey(WRITE_TIMEOUT_FILTER)) {
            transport = new WriteTimeoutFilter(transport);
            String soWriteTimeout = (String) options.remove(WRITE_TIMEOUT_FILTER);
            if (soWriteTimeout != null) {
                ((WriteTimeoutFilter) transport).setWriteTimeout(Long.parseLong(soWriteTimeout));
            }
        }
        // 将URI的配置参数通过反射配置到transport上
        IntrospectionSupport.setProperties(transport, options);
        return transport;
    }

    @SuppressWarnings("rawtypes")
    protected String getOption(Map options, String key, String def) {
        String rc = (String) options.remove(key);
        if (rc == null) {
            rc = def;
        }
        return rc;
    }
}
