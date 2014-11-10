/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.replication;


/**
 * A factory for constructing {@link Applier}s using an {@link ApplierConfiguration}.
 */
public interface ApplierFactory {

    /**
     * Creates an applier that writes to the specified region and table.
     *
     * @param config
     *            {@link TableConfiguration} that includes all information required to construct an {@link Applier}.
     * @return an instance of Applier that writes to the specified region and table
     */
    Applier createApplier(TableConfiguration config);

}
