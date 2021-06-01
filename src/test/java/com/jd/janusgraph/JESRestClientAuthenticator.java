package com.jd.janusgraph;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticatorBase;

import java.io.IOException;

public class JESRestClientAuthenticator extends RestClientAuthenticatorBase  {
    private HttpRequestInterceptor interceptor;
    public JESRestClientAuthenticator(String[] args){}
    @Override
    public void init() throws IOException {
        interceptor=new HttpRequestInterceptor() {
            @Override
            public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
                request.addHeader("jes-user","galaxy-jtlas-app");
                request.addHeader("jes-password","c9f03dff789318d91b35ef0fc26fed33");
            }
        };
    }
    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        return   httpClientBuilder.addInterceptorLast(interceptor);
    }

}
