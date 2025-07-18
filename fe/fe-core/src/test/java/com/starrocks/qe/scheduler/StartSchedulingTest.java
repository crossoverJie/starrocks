// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.qe.scheduler;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Reference;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.GlobalDictNotMatchException;
import com.starrocks.proto.PCancelPlanFragmentRequest;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.rpc.PExecPlanFragmentRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.thrift.TException;
import org.assertj.core.util.Sets;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.utframe.MockedBackend.MockPBackendService;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The procedure of starting scheduling is as follows.
 * <pre>{@code
 * Deploy instances -> Wait for deployment futures       ----> getNext -> Wait for reports
 *      | RPC throws exception | Future::get throws error    | wrong packetSeq or
 *      |                      |         or                  | RPC throws exception or
 *      |                      | Future::get timeout         | Future::get throws error or
 *      |                      |         or                  | Reach limit
 *      |                      | Future returns error status |
 *      |                      V                             |
 *      ------------>     Cancel the deployed instances <-----
 * }</pre>
 * *
 */
public class StartSchedulingTest extends SchedulerTestBase {
    private boolean originalEnableProfile;

    @BeforeEach
    public void before() {
        originalEnableProfile = connectContext.getSessionVariable().isEnableProfile();
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setEnableProfile(originalEnableProfile);
    }

    @Test
    public void testDeploySuccess() throws Exception {
        Map<TNetworkAddress, Integer> backendToNumInstances = Maps.newHashMap();
        Map<TNetworkAddress, List<TExecPlanFragmentParams>> backendToRequests = Maps.newHashMap();
        Map<Integer, List<TExecPlanFragmentParams>> fragmentToRequest = Maps.newHashMap();
        setBackendService(address -> new MockPBackendService() {
            @Override
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                TExecPlanFragmentParams tRequest = new TExecPlanFragmentParams();
                try {
                    request.getRequest(tRequest);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                backendToRequests.computeIfAbsent(address, (k) -> Lists.newArrayList()).add(tRequest);

                int rootNodeId = tRequest.getFragment().getPlan().getNodes().get(0).getNode_id();
                fragmentToRequest.computeIfAbsent(rootNodeId, (k) -> Lists.newArrayList()).add(tRequest);

                // Check cache desc table.
                backendToNumInstances.compute(address, (k, v) -> {
                    if (v == null) {
                        Assertions.assertFalse(tRequest.desc_tbl.isIs_cached());
                        Assertions.assertFalse(tRequest.desc_tbl.getTupleDescriptors().isEmpty());
                        return 1;
                    } else {
                        Assertions.assertTrue(tRequest.desc_tbl.isIs_cached());
                        Assertions.assertTrue(tRequest.desc_tbl.getTupleDescriptors().isEmpty());
                        return v + 1;
                    }
                });

                return super.execPlanFragmentAsync(request);
            }
        });

        String sql = "select count(1) from lineitem UNION ALL select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);

        Assertions.assertTrue(scheduler.getExecStatus().ok());

        // Check instance number.
        backendToRequests.forEach((address, requests) -> requests.forEach(req ->
                Assertions.assertEquals(backendToNumInstances.get(address).intValue(), req.getParams().getInstances_number())));

        // Check backend number.
        fragmentToRequest.values().forEach(requestsOfFragment -> {
            List<TExecPlanFragmentParams> requestsOrderedByBackendNum = new ArrayList<>(requestsOfFragment);
            List<TExecPlanFragmentParams> requestsOrderedByInstanceId = new ArrayList<>(requestsOfFragment);
            requestsOrderedByBackendNum.sort(Comparator.comparingInt(TExecPlanFragmentParams::getBackend_num));
            requestsOrderedByInstanceId.sort(Comparator.comparing(req -> req.getParams().getFragment_instance_id()));
            assertThat(requestsOrderedByBackendNum).containsExactlyElementsOf(requestsOrderedByInstanceId);
        });
    }

    @Test
    public void testDeployThrowException() {
        setBackendService(address -> {
            if (!backend3.getHost().equals(address.getHostname())) {
                return new MockPBackendService();
            }
            return new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    throw new RuntimeException("test runtime exception");
                }
            };
        });

        String sql = "select count(1) from lineitem";

        Assertions.assertThrows(RpcException.class, () -> startScheduling(sql), "test runtime exception");
        SimpleScheduler.removeFromBlocklist(backend3.getId());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !SimpleScheduler.isInBlocklist(backend3.getId()));
    }

    @Test
    public void testDeployFutureThrowException() throws Exception {
        connectContext.getSessionVariable().setEnableProfile(true);

        AtomicBoolean isFirstFragmentToDeploy = new AtomicBoolean(true);
        Reference<Future<PExecPlanFragmentResult>> deployFuture = new Reference<>();
        setBackendService(address -> {
            if (isFirstFragmentToDeploy.get() || !backend3.getHost().equals(address.getHostname())) {
                isFirstFragmentToDeploy.set(false);
                return new MockPBackendService();
            }
            return new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    return deployFuture.getRef();
                }
            };
        });

        String sql = "select count(1) from lineitem t1 JOIN [shuffle] lineitem t2 using(l_orderkey)";

        deployFuture.setRef(
                mockFutureWithException(new ExecutionException("test execution exception", new Exception())));
        Assertions.assertThrows(RpcException.class, () -> startScheduling(sql), "test execution exception");
        SimpleScheduler.removeFromBlocklist(backend3.getId());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !SimpleScheduler.isInBlocklist(backend3.getId()));

        isFirstFragmentToDeploy.set(true);
        deployFuture.setRef(mockFutureWithException(new InterruptedException("test interrupted exception")));
        DefaultCoordinator scheduler = getScheduler(sql);
        Assertions.assertThrows(StarRocksException.class, () -> scheduler.exec(), "test interrupted exception");

        // The deployed executions haven't reported.
        Assertions.assertFalse(scheduler.isDone());

        // Shouldn't deploy the rest instances, when the previous instance deployment failed.
        ExecutionDAG executionDAG = scheduler.getExecutionDAG();
        Assertions.assertTrue(executionDAG.getExecutions().size() < executionDAG.getInstances().size());
        // Receive execution reports.
        executionDAG.getExecutions().forEach(execution -> {
            TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
            request.setBackend_num(execution.getIndexInJob());
            request.setDone(true);
            request.setStatus(new TStatus(TStatusCode.CANCELLED));
            request.setFragment_instance_id(execution.getInstanceId());

            scheduler.updateFragmentExecStatus(request);
        });
        Assertions.assertTrue(scheduler.isDone());
    }

    @Test
    public void testDeployFutureThrowRetryableException() throws Exception {
        AtomicInteger numDeployedFragments = new AtomicInteger(0);

        setBackendService(address -> {

            final int numFragments = numDeployedFragments.incrementAndGet();
            if (numFragments == 1) {
                return new MockPBackendService();
            }

            if (numFragments == 2) {
                return new MockPBackendService() {
                    @Override
                    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                        return submit(() -> {
                            PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                            StatusPB pStatus = new StatusPB();
                            pStatus.statusCode = TStatusCode.CANCELLED.getValue();
                            pStatus.errorMsgs = Collections.singletonList("test CANCELLED error message");
                            result.status = pStatus;
                            return result;
                        });
                    }
                };
            }

            if (numFragments == 3) {
                return new MockPBackendService() {
                    @Override
                    public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                        return submit(() -> {
                            PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                            StatusPB pStatus = new StatusPB();
                            pStatus.statusCode = TStatusCode.GLOBAL_DICT_NOT_MATCH.getValue();
                            pStatus.errorMsgs = Collections.singletonList("test GLOBAL_DICT_NOT_MATCH error message");
                            result.status = pStatus;
                            return result;
                        });
                    }
                };
            }

            return new MockPBackendService();
        });
        String sql =
                "select count(1) from lineitem UNION ALL select count(1) from lineitem UNION ALL select count(1) from lineitem";
        DefaultCoordinator scheduler = getScheduler(sql);
        Assertions.assertThrows(GlobalDictNotMatchException.class, scheduler::exec);
    }

    @Test
    public void testDeployFutureReturnErrorStatus() throws Exception {
        final int successDeployedFragmentCount = 3;
        Set<TUniqueId> deployedInstanceIds = Sets.newHashSet();
        Set<TUniqueId> cancelledInstanceIds = Sets.newHashSet();
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                if (deployedInstanceIds.size() < successDeployedFragmentCount) {
                    TExecPlanFragmentParams tRequest = new TExecPlanFragmentParams();
                    try {
                        request.getRequest(tRequest);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }
                    deployedInstanceIds.add(tRequest.getParams().getFragment_instance_id());
                    return super.execPlanFragmentAsync(request);
                }

                return submit(() -> {
                    PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                    StatusPB pStatus = new StatusPB();
                    pStatus.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                    pStatus.errorMsgs = Collections.singletonList("test error message");
                    result.status = pStatus;
                    return result;
                });
            }

            @Override
            public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
                TUniqueId instanceId = new TUniqueId(request.finstId.hi, request.finstId.lo);
                cancelledInstanceIds.add(instanceId);

                return super.cancelPlanFragmentAsync(request);
            }
        });

        String sql =
                "select count(1) from lineitem UNION ALL select count(1) from lineitem UNION ALL select count(1) from lineitem";
        DefaultCoordinator scheduler = getScheduler(sql);
        Assertions.assertThrows(StarRocksException.class, scheduler::exec, "test error message");

        // All the deployed fragment instances should be cancelled.
        Assertions.assertEquals(successDeployedFragmentCount, cancelledInstanceIds.size());
        assertThat(cancelledInstanceIds).containsExactlyElementsOf(deployedInstanceIds);
    }

    @Test
    public void testDeployTimeout() throws Exception {
        int prevQueryDeliveryTimeoutSecond = connectContext.getSessionVariable().getQueryDeliveryTimeoutS();

        try {
            connectContext.getSessionVariable().setQueryDeliveryTimeoutS(1);

            setBackendService(new MockPBackendService() {
                @Override
                public Future<PExecPlanFragmentResult> execPlanFragmentAsync(PExecPlanFragmentRequest request) {
                    return submit(() -> {
                        try {
                            Thread.sleep(5_000L); // NOSONAR
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                        PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                        StatusPB pStatus = new StatusPB();
                        pStatus.statusCode = 0;
                        result.status = pStatus;
                        return result;
                    });
                }
            });

            String sql = "select count(1) from lineitem t1 JOIN [shuffle] lineitem t2 using(l_orderkey)";
            DefaultCoordinator scheduler = getScheduler(sql);
            Assertions.assertThrows(StarRocksException.class, () -> scheduler.exec(), "deploy query timeout");
        } finally {
            connectContext.getSessionVariable().setQueryDeliveryTimeoutS(prevQueryDeliveryTimeoutSecond);
        }
    }

    @Test
    public void testCancelThrowErrors() throws Exception {
        Set<TUniqueId> successCancelledInstanceIds = Sets.newHashSet();
        final int numSuccessCancelledInstances = 3;
        setBackendService(new MockPBackendService() {
            @Override
            public Future<PCancelPlanFragmentResult> cancelPlanFragmentAsync(PCancelPlanFragmentRequest request) {
                if (successCancelledInstanceIds.size() < numSuccessCancelledInstances) {
                    TUniqueId instanceId = new TUniqueId(request.finstId.hi, request.finstId.lo);
                    successCancelledInstanceIds.add(instanceId);
                    return super.cancelPlanFragmentAsync(request);
                } else {
                    throw new RuntimeException("mock-runtime-exception-when-cancel-plan-fragment-async");
                }
            }
        });

        String sql = "select count(1) from lineitem UNION ALL select count(1) from lineitem";
        DefaultCoordinator scheduler = startScheduling(sql);
        Assertions.assertTrue(scheduler.getExecStatus().ok());
        ExecutionDAG executionDAG = scheduler.getExecutionDAG();

        // All the instances should be deployed.
        Assertions.assertEquals(executionDAG.getInstances().size(), executionDAG.getExecutions().size());

        scheduler.cancel("Cancel by test");
        Assertions.assertEquals(numSuccessCancelledInstances, successCancelledInstanceIds.size());
        // Receive execution reports from the successfully cancelled instances.
        executionDAG.getExecutions().forEach(execution -> {
            if (successCancelledInstanceIds.contains(execution.getInstanceId())) {
                TReportExecStatusParams request = new TReportExecStatusParams(FrontendServiceVersion.V1);
                request.setBackend_num(execution.getIndexInJob());
                request.setDone(true);
                request.setStatus(new TStatus(TStatusCode.CANCELLED));
                request.setFragment_instance_id(execution.getInstanceId());

                scheduler.updateFragmentExecStatus(request);
            }
        });
        // Shouldn't block by the failed cancelled instance.
        Assertions.assertTrue(scheduler.isDone());

        SimpleScheduler.removeFromBlocklist(BACKEND1_ID);
        SimpleScheduler.removeFromBlocklist(backend2.getId());
        SimpleScheduler.removeFromBlocklist(backend3.getId());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() ->
                !SimpleScheduler.isInBlocklist(BACKEND1_ID) && !SimpleScheduler.isInBlocklist(backend2.getId()) &&
                        !SimpleScheduler.isInBlocklist(backend3.getId()));
    }

    private static Future<PExecPlanFragmentResult> mockFutureWithException(Exception exception) {

        return new Future<PExecPlanFragmentResult>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public PExecPlanFragmentResult get() {
                return null;
            }

            @Override
            public PExecPlanFragmentResult get(long timeout, @NotNull TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                if (exception instanceof InterruptedException) {
                    throw (InterruptedException) exception;
                } else if (exception instanceof ExecutionException) {
                    throw (ExecutionException) exception;
                } else if (exception instanceof TimeoutException) {
                    throw (TimeoutException) exception;
                } else {
                    throw new IllegalArgumentException("mockFutureWithException with illegal exception: " + exception);
                }
            }
        };
    }

}
