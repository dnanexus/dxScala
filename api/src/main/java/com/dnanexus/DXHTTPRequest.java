// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

package com.dnanexus;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.BasicHttpContext;

import com.dnanexus.exceptions.DXAPIException;
import com.dnanexus.exceptions.DXHTTPException;
import com.dnanexus.exceptions.InternalErrorException;
import com.dnanexus.exceptions.ServiceUnavailableException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Class for making a raw DNAnexus API call via HTTP.
 */
public class DXHTTPRequest {
    /**
     * Holds either the raw text of a response or a parsed JSON version of it.
     */
    private static class ParsedResponse {
        public final String responseText;
        public final JsonNode responseJson;

        public ParsedResponse(String responseText, JsonNode responseJson) {
            this.responseText = responseText;
            this.responseJson = responseJson;
        }
    }

    /**
     * Indicates whether a particular API request can be retried.
     *
     * <p>
     * See the <a
     * href="https://github.com/dnanexus/dx-toolkit/blob/master/src/api_wrappers/README.md">API
     * wrappers common documentation</a> for the retry logic specification.
     * </p>
     */
    public static enum RetryStrategy {
        /**
         * The request has non-idempotent side effects and is generally not safe to retry if the
         * outcome of a previous request is unknown.
         */
        UNSAFE_TO_RETRY,
        /**
         * The request is idempotent and is safe to retry.
         */
        SAFE_TO_RETRY;
    }

    /**
     * Sleeps for the specified amount of time. Throws a {@link RuntimeException} if interrupted.
     *
     * @param seconds number of seconds to sleep for
     */
    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private final JsonNode securityContext;

    private final String apiserver;

    protected final HttpClient httpclient;

    private final boolean disableRetry;

    private static final int NUM_RETRIES = 10;

    private static final DXEnvironment defaultEnv = DXEnvironment.create();

    private static final String USER_AGENT = DXUserAgent.getUserAgent();

    // consider moving this to DXEnvironment.java
    private static final String debugLevel = System.getenv("_DX_DEBUG") == null ? "0": System.getenv("_DX_DEBUG");

    private static String errorMessage(String method, String resource, String errorString,
            int retryWait, int nextRetryNum, int maxRetries) {
        String baseError = method + " " + resource + ": " + errorString + ".";
        if (nextRetryNum <= maxRetries) {
            return baseError + "  Waiting " + retryWait + " seconds before retry " + nextRetryNum
                    + " of " + maxRetries;
        }
        return baseError;
    }

    /**
     * Prints an error message to stderr when _DX_DEBUG env is set as > 0.
     *
     * @param msg the error message to be printed
     *
     */
    private static void logError(String msg) {

        // check that if debug level is not set (null), set it as "0"
        if (!debugLevel.equals("0")) {
            System.err.println("[" + System.currentTimeMillis() + "] " + msg);
        }
    }

    /**
     * Returns the value of a given header from an HttpResponse
     *
     * @param response the HttpResponse Object
     *
     * @param headerName name of the header to extract the value
     */
    private static String getHeader(HttpResponse response, String headerName) {
        String headerValue = "";
        if (response.containsHeader(headerName)) {
            headerValue = response.getFirstHeader(headerName).getValue();
        }
        return headerValue;
    }

    /**
     * Construct the DXHTTPRequest using the default DXEnvironment.
     */
    public DXHTTPRequest() {
        this(defaultEnv);
    }

    /**
     * Construct the DXHTTPRequest using the given DXEnvironment.
     */
    public DXHTTPRequest(DXEnvironment env) {
        this.securityContext = env.getSecurityContextJson();
        this.apiserver = env.getApiserverPath();
        this.disableRetry = env.isRetryDisabled();

        // These timeouts prevent requests from getting stuck
        RequestConfig.Builder reqBuilder = RequestConfig.custom()
            .setConnectTimeout(env.getConnectionTimeout())
            .setSocketTimeout(env.getSocketTimeout());

        DXEnvironment.ProxyDesc proxyDesc = env.getProxy();
        if (proxyDesc == null) {
            RequestConfig requestConfig = reqBuilder.build();
            this.httpclient = HttpClientBuilder.create().setUserAgent(USER_AGENT).setDefaultRequestConfig(requestConfig).build();
            return;
        }

          // Configure a proxy
        if (!proxyDesc.authRequired) {
            reqBuilder.setProxy(proxyDesc.host);
            RequestConfig requestConfig = reqBuilder.build();
            this.httpclient = HttpClientBuilder.create().setUserAgent(USER_AGENT).setDefaultRequestConfig(requestConfig).build();
            return;
        }

        // We need to authenticate with a username and password.
        reqBuilder.setProxy(proxyDesc.host);

        // specify the user/password in the configuration
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        if (proxyDesc.method != null && proxyDesc.method.equals("ntlm")) {
            // NTLM: windows NT authentication, with Kerberos
            String localHostname;
            try {
                localHostname = java.net.InetAddress.getLocalHost().getHostName();
            } catch (java.net.UnknownHostException e) {
                throw new RuntimeException(e);
            }
            credsProvider.setCredentials(
                new AuthScope(proxyDesc.host.getHostName(),
                              proxyDesc.host.getPort(),
                              AuthScope.ANY_REALM,
                              "ntlm"),
                new NTCredentials(proxyDesc.username,
                                  proxyDesc.password,
                                  localHostname,
                                  proxyDesc.domain));
        } else {
            // Default authentication
            credsProvider.setCredentials(new AuthScope(proxyDesc.host),
                                         new UsernamePasswordCredentials(proxyDesc.username,
                                                                         proxyDesc.password));
        }

        RequestConfig requestConfig = reqBuilder.build();
        this.httpclient = HttpClientBuilder.create()
            .setDefaultCredentialsProvider(credsProvider)
            .setUserAgent(USER_AGENT)
            .setDefaultRequestConfig(requestConfig)
            .build();
    }

    /**
     * Allows custom DXHTTPRequest to setup overridden httpClient
     */
    protected HttpClient getHttpClient() {
        return this.httpclient;
    }

    /**
     * Allows custom HttpContext to setup overridden http. parameters
     * e.g. http.conn-manager.timeout for PoolingHttpClientConnectionManager
     */
    protected HttpContext getHttpContext() { return new BasicHttpContext(); }

    @Deprecated
    public JsonNode request(String resource, JsonNode data) {
        return request(resource, data, RetryStrategy.SAFE_TO_RETRY);
    }

    public JsonNode request(String resource, JsonNode data, RetryStrategy retryStrategy) {
        String dataAsString = data.toString();
        return requestImpl(resource, dataAsString, true, retryStrategy).responseJson;
    }

    @Deprecated
    public String request(String resource, String data) {
        return request(resource, data, RetryStrategy.SAFE_TO_RETRY);
    }

    public String request(String resource, String data, RetryStrategy retryStrategy) {
        return requestImpl(resource, data, false, retryStrategy).responseText;
    }

    /**
     * Issues a request against the specified resource and returns either the text of the response
     * or the parsed JSON of the response (depending on whether parseResponse is set).
     *
     * @throws DXAPIException If the server returns a complete response with an HTTP status code
     *         other than 200 (OK).
     * @throws DXHTTPException If an error occurs while making the HTTP request or obtaining the
     *         response (includes HTTP protocol errors).
     * @throws InternalError If the server returns an HTTP status code 500 and the environment
     *         specifies that retries are disabled.
     * @throws ServiceUnavailableException If the server returns an HTTP status code 503 and
     *         indicates that the client should retry the request at a later time, and the
     *         environment specifies that retries are disabled.
     */
    private ParsedResponse requestImpl(String resource, String data, boolean parseResponse,
            RetryStrategy retryStrategy) {

        HttpPost request = new HttpPost(apiserver + resource);

        if (securityContext == null || securityContext.isNull()) {
            throw new DXHTTPException(new IOException("No security context was set"));
        }

        request.setHeader("Content-Type", "application/json");
        request.setHeader("Connection", "close");
        request.setHeader("Authorization", securityContext.get("auth_token_type").textValue() + " "
                + securityContext.get("auth_token").textValue());
        request.setEntity(new StringEntity(data, Charset.forName("UTF-8")));

        // Retry with exponential backoff
        int timeoutSeconds = 1;
        int attempts = 0;

        while (true) {
            Integer statusCode = null;
            String requestId = "";

            // This guarantees that we get at least one iteration around this loop before running
            // out of retries, so we can check at the bottom of the loop instead of the top.
            assert NUM_RETRIES > 0;

            // By default, our conservative strategy is to retry if the route permits it. Later we
            // may update this to unconditionally retry if we can definitely determine that the
            // server never saw the request.
            boolean retryRequest = (retryStrategy == RetryStrategy.SAFE_TO_RETRY);
            int retryAfterSeconds = 60;

            try {
                // In this block, any IOException will cause the request to be retried (up to a
                // total of NUM_RETRIES retries). RuntimeException (including DXAPIException)
                // instances are not caught and will immediately return control to the caller.

                // TODO: distinguish between errors during connection init and socket errors while
                // sending or receiving data. The former can always be retried, but the latter can
                // only be retried if the request is idempotent.
                HttpResponse response = getHttpClient().execute(request, getHttpContext());

                statusCode = response.getStatusLine().getStatusCode();
                requestId = getHeader(response, "X-Request-ID");
                HttpEntity entity = response.getEntity();

                if (statusCode == null) {
                    throw new DXHTTPException();
                } else if (statusCode == HttpStatus.SC_OK) {
                    // 200 OK
                    byte[] value = EntityUtils.toByteArray(entity);
                    int realLength = value.length;
                    if (entity.getContentLength() >= 0 && realLength != entity.getContentLength()) {
                        // Content length mismatch. Retry is possible (if the route permits it).
                        throw new IOException("Received response of " + realLength
                                + " bytes but Content-Length was " + entity.getContentLength());
                    } else if (parseResponse) {
                        JsonNode responseJson = null;
                        try {
                            responseJson = DXJSON.parseJson(new String(value, "UTF-8"));
                        } catch (JsonProcessingException e) {
                            if (entity.getContentLength() < 0) {
                                // content-length was not provided, and the JSON could not be
                                // parsed. Retry (if the route permits it) since this is probably
                                // just a streaming request that encountered a transient error.
                                throw new IOException(
                                        "Content-length was not provided and the response JSON could not be parsed.");
                            }
                            // This is probably a real problem (the request
                            // is complete but doesn't parse), so avoid
                            // masking it as an IOException (which is
                            // rethrown as DXHTTPException below). If it
                            // comes up frequently we can revisit how these
                            // should be handled.
                            throw new RuntimeException(
                                    "Request is of the correct length but is unparseable", e);
                        } catch (IOException e) {
                            // TODO: characterize what kinds of errors
                            // DXJSON.parseJson can emit, determine how we can
                            // get here and what to do about it.
                            throw new RuntimeException(e);
                        }
                        return new ParsedResponse(null, responseJson);
                    } else {
                        return new ParsedResponse(new String(value, Charset.forName("UTF-8")), null);
                    }
                } else if (statusCode < 500) {
                    // 4xx errors should be considered not recoverable.
                    String responseStr = EntityUtils.toString(entity);
                    String errorType = null;
                    String errorMessage = responseStr;
                    try {
                        JsonNode responseJson = DXJSON.parseJson(responseStr);
                        JsonNode errorField = responseJson.get("error");
                        if (errorField != null) {
                            JsonNode typeField = errorField.get("type");
                            if (typeField != null) {
                                errorType = typeField.asText();
                            }
                            JsonNode messageField = errorField.get("message");
                            if (messageField != null) {
                                errorMessage = messageField.asText();
                            }
                        }
                    } catch (IOException e) {
                        // Just fall back to reproducing the entire response
                        // body.
                    }
                    logError(errorType + ": " + errorMessage + ". Code: " + Integer.toString(statusCode)
                        + " Request ID: " + requestId);
                    throw DXAPIException.getInstance(errorType, errorMessage, statusCode);
                } else {
                    // Propagate 500 error to caller
                    if (this.disableRetry && statusCode != 503) {
                        logError("POST " + resource + ": " + statusCode + " Internal Server Error, try "
                                + String.valueOf(attempts + 1) + "/" + NUM_RETRIES
                                + " Request ID: " +  requestId);
                        throw new InternalErrorException("Internal Server Error", statusCode);
                    }
                    // If retries enabled, 500 InternalError should get retried unconditionally
                    retryRequest = true;
                    if (statusCode == 503) {
                        Header retryAfterHeader = response.getFirstHeader("retry-after");
                        // Consume the response to avoid leaking resources
                        EntityUtils.consume(entity);
                        if (retryAfterHeader != null) {
                            try {
                                retryAfterSeconds = Integer.parseInt(retryAfterHeader.getValue());
                            } catch (NumberFormatException e) {
                                // Just fall back to the default
                            }
                        }
                        throw new ServiceUnavailableException("503 Service Unavailable", statusCode, retryAfterSeconds);
                    }
                    throw new IOException(EntityUtils.toString(entity));
                }
            } catch (ServiceUnavailableException e) {
                int secondsToWait = retryAfterSeconds;

                if (this.disableRetry) {
                    logError("POST " + resource + ": 503 Service Unavailable, suggested wait "
                            + secondsToWait + " seconds" + ". Request ID: " +  requestId);
                    throw e;
                }

                // Retries due to 503 Service Unavailable and Retry-After do NOT count against the
                // allowed number of retries.
                logError("POST " + resource + ": 503 Service Unavailable, waiting for "
                        + Integer.toString(secondsToWait) + " seconds" + " Request ID: " +  requestId);
                sleep(secondsToWait);
                continue;
            } catch (IOException e) {
                // Note, this catches both exceptions directly thrown from httpclient.execute (e.g.
                // no connectivity to server) and exceptions thrown by our code above after parsing
                // the response.
                logError(errorMessage("POST", resource, e.toString(), timeoutSeconds,
                        attempts + 1, NUM_RETRIES));
                if (attempts == NUM_RETRIES || !retryRequest) {
                    if (statusCode == null) {
                        throw new DXHTTPException();
                    }
                    throw new InternalErrorException("Maximum number of retries reached, or unsafe to retry",
                            statusCode);
                }
            }

            assert attempts < NUM_RETRIES;
            assert retryRequest;

            attempts++;

            // The number of failed attempts is now no more than NUM_RETRIES, and the total number
            // of attempts allowed is NUM_RETRIES + 1 (the first attempt, plus up to NUM_RETRIES
            // retries). So there is at least one more retry left; sleep before we retry.
            assert attempts <= NUM_RETRIES;


            if (this.disableRetry)
                throw new RuntimeException("Retry disabled");


            sleep(timeoutSeconds);
            timeoutSeconds *= 2;
        }
    }
}
