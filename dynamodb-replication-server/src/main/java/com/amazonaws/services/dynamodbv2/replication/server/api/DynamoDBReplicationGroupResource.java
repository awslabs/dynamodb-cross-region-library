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
package com.amazonaws.services.dynamodbv2.replication.server.api;

import java.io.IOException;
import java.util.UUID;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AddReplicationGroupMemberRequest;
import com.amazonaws.services.dynamodbv2.model.CreateReplicationGroupRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteReplicationGroupRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeReplicationGroupRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeReplicationGroupResult;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationCoordinatorRequests;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroup;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMember;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupMemberStatus;
import com.amazonaws.services.dynamodbv2.model.DynamoDBReplicationGroupStatus;
import com.amazonaws.services.dynamodbv2.model.ListReplicationGroupsRequest;
import com.amazonaws.services.dynamodbv2.model.RemoveReplicationGroupMemberRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateReplicationGroupMemberRequest;
import com.amazonaws.services.dynamodbv2.replication.DynamoDBMetadataStorage;
import com.amazonaws.services.dynamodbv2.replication.server.DynamoDBReplicationServer;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Resource class for replication coordinator server
 *
 */
@Path("/")
public class DynamoDBReplicationGroupResource {

    /*
     * Logger for the class
     */
    private static final Logger LOGGER = Logger.getLogger(DynamoDBReplicationGroupResource.class);

    /*
     * DynamoDBReplicationObjectMapper needed to parse requests
     */
    private static final ObjectMapper MAPPER = DynamoDBReplicationObjectMapper.getInstance();

    /**
     * process coordinator request
     *
     * @param rawrequest
     *            the request should conform to the {@link DynamoDBReplicationCoordinatorRequests} model
     * @return 200 OK success if group was successfully created in the metadata table, error otherwise
     * @throws JsonParseException
     *             error parsing the raw string request
     * @throws JsonMappingException
     *             error mapping the json to a {@link CreateReplicationGroupRequest} object
     * @throws IOException
     *             error accessing the metadata table
     */
    @POST
    public Response processRequest(String rawrequest) {
        // prepare the response builder and error message for failed requests
        ResponseBuilder failedBuilder = Response.status(Response.Status.INTERNAL_SERVER_ERROR);
        String errMessage = "Internal server error";
        try {
            // parse the raw request to a generic DynamoDBReplicationGroupAPI object
            DynamoDBReplicationGroupAPI request = MAPPER.readValue(rawrequest, DynamoDBReplicationGroupAPI.class);

            // prepare the metadata table access
            DynamoDBMetadataStorage md = DynamoDBMetadataStorage.getInstance();

            // prepare the response builder for successful requests
            ResponseBuilder successBuilder = Response.status(Response.Status.OK);

            // prepare existing replication group instances
            DynamoDBReplicationGroup existingGroup = null;
            DynamoDBReplicationGroup updatedGroup = null;

            // parse the command and execute
            switch (DynamoDBReplicationCoordinatorRequests.valueOf(request.getCommand())) {
                case AddReplicationGroupMemberRequest:
                    // convert the given command arguments to the actual request class object
                    AddReplicationGroupMemberRequest addMemberRequest = MAPPER.convertValue(request.getCommandArguments(),
                        AddReplicationGroupMemberRequest.class);

                    // check the version of the current group against the expected version
                    existingGroup = checkVersion(md, request.getVersion(), addMemberRequest.getReplicationGroupUUID());
                    if (existingGroup == null) {
                        errMessage = "replication group version does not match!";
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // create a new replication group member based on the given request
                    DynamoDBReplicationGroupMember newMember = new DynamoDBReplicationGroupMember(addMemberRequest);
                    newMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING);

                    // make sure the replication group member does not already exist
                    errMessage = "Cannot add a new replication group member with the same name as an existing replication group member: "
                        + addMemberRequest.getMemberArn();
                    if (existingGroup.getReplicationGroupMembers().get(addMemberRequest.getMemberArn()) != null) {
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // add the new member to the updated replication group
                    updatedGroup = new DynamoDBReplicationGroup(existingGroup);
                    updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);
                    updatedGroup.addReplicationGroupMember(newMember);

                    // try to write the updated group to the metadata table
                    errMessage = "error writing the replication group to the table " + updatedGroup;
                    if (updatedGroup.equals(md.compareAndWriteReplicationGroup(existingGroup, updatedGroup))) {
                        return successBuilder.build();
                    } else {
                        LOGGER.error(errMessage);
                        return failedBuilder.encoding(errMessage).build();
                    }
                case CreateReplicationGroupRequest:
                    // convert the given command arguments to the actual request class object
                    CreateReplicationGroupRequest createRequest = MAPPER.convertValue(request.getCommandArguments(), CreateReplicationGroupRequest.class);

                    // generate a replication group UUID if there is none specified
                    if (createRequest.getReplicationGroupUUID() == null || createRequest.getReplicationGroupUUID().isEmpty()) {
                        createRequest.setReplicationGroupUUID(UUID.randomUUID().toString());
                    }

                    // construct new DynamoDBReplicationGroup based on the request
                    DynamoDBReplicationGroup newGroup = new DynamoDBReplicationGroup(createRequest);

                    // change group and group member status to creating
                    DynamoDBReplicationGroup creatingGroup = getCreatingGroup(newGroup);

                    // make sure the replication group does not already exist
                    errMessage = "Cannot create a new replication group with the same UUID as an existing replication group: "
                        + createRequest.getReplicationGroupUUID();
                    if (md.readReplicationGroup(createRequest.getReplicationGroupUUID()) != null) {
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // try to write the new group to the metadata table
                    errMessage = "error writing the replication group to the table " + newGroup;
                    if (creatingGroup.equals(md.compareAndWriteReplicationGroup(null /* no expected value for new group */, creatingGroup))) {
                        return successBuilder.build();
                    } else {
                        LOGGER.error(errMessage);
                        return failedBuilder.encoding(errMessage).build();
                    }
                case DeleteReplicationGroupRequest:
                    // convert the given command arguments to the actual request class object
                    DeleteReplicationGroupRequest deleteRequest = MAPPER.convertValue(request.getCommandArguments(), DeleteReplicationGroupRequest.class);

                    // check the version of the current group against the expected version
                    existingGroup = checkVersion(md, request.getVersion(), deleteRequest.getReplicationGroupUUID());
                    if (existingGroup == null) {
                        errMessage = "replication group does not exist or replication group version does not match!";
                        return failedBuilder.entity(errMessage).build();
                    }

                    // change group and group member status to deleting
                    DynamoDBReplicationGroup deletingGroup = getDeletingGroup(existingGroup);

                    // try to write the updated group to the metadata table
                    errMessage = "error writing the updated replication group to the table" + deletingGroup;
                    if (deletingGroup.equals(md.compareAndWriteReplicationGroup(existingGroup, deletingGroup))) {
                        successBuilder.build();
                    } else {
                        LOGGER.error(errMessage);
                        return failedBuilder.encoding(errMessage).build();
                    }
                case DescribeReplicationGroupRequest:
                    // convert the given command arguments to the actual request class object
                    DescribeReplicationGroupRequest describeRequest = MAPPER.convertValue(request.getCommandArguments(), DescribeReplicationGroupRequest.class);

                    // read the replication group from metadata table
                    DynamoDBReplicationGroup resultingGroup = md.readReplicationGroup(describeRequest.getReplicationGroupUUID());

                    return successBuilder.entity(new DescribeReplicationGroupResult().withReplicationGroup(resultingGroup)).build();
                case ListReplicationGroupsRequest:
                    // convert the given command arguments to the actual request class object
                    /* ListReplicationGroupsRequest listRequest = */MAPPER.convertValue(request.getCommandArguments(), ListReplicationGroupsRequest.class);
                    // TODO Page results and implement limit parameter.
                    return successBuilder.entity(md.readReplicationGroups()).build();
                case RemoveReplicationGroupMemberRequest:
                    // convert the given command arguments to the actual request class object
                    RemoveReplicationGroupMemberRequest removeMemberRequest = MAPPER.convertValue(request.getCommandArguments(),
                        RemoveReplicationGroupMemberRequest.class);

                    // check the version of the current group against the expected version
                    existingGroup = checkVersion(md, request.getVersion(), removeMemberRequest.getReplicationGroupUUID());
                    if (existingGroup == null) {
                        errMessage = "replication group version does not match!";
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // make sure the replication group member exists
                    errMessage = "Cannot remove a non-existent replication group member: " + removeMemberRequest.getMemberArn();
                    if (existingGroup.getReplicationGroupMembers().get(removeMemberRequest.getMemberArn()) == null) {
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // update status of the group and of the member to be removed
                    updatedGroup = new DynamoDBReplicationGroup(existingGroup);
                    updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);
                    updatedGroup.getReplicationGroupMembers().get(removeMemberRequest.getMemberArn())
                        .setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);

                    // try to write the updated group to the metadata table
                    errMessage = "error writing the replication group to the table " + updatedGroup;
                    if (updatedGroup.equals(md.compareAndWriteReplicationGroup(existingGroup, updatedGroup))) {
                        successBuilder.build();
                    } else {
                        LOGGER.error(errMessage);
                        return failedBuilder.encoding(errMessage).build();
                    }
                case UpdateReplicationGroupMemberRequest:
                    // convert the given command arguments to the actual request class object
                    UpdateReplicationGroupMemberRequest updateMemberRequest = MAPPER.convertValue(request.getCommandArguments(),
                        UpdateReplicationGroupMemberRequest.class);

                    // check the version of the current group against the expected version
                    existingGroup = checkVersion(md, request.getVersion(), updateMemberRequest.getReplicationGroupUUID());
                    if (existingGroup == null) {
                        errMessage = "replication group version does not match!";
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // make sure the replication group member exists
                    errMessage = "Cannot update a non-existent replication group member: " + updateMemberRequest.getMemberArn();
                    if (existingGroup.getReplicationGroupMembers().get(updateMemberRequest.getMemberArn()) == null) {
                        LOGGER.error(errMessage);
                        return failedBuilder.entity(errMessage).build();
                    }

                    // update the connectors of the group and status of the group and member
                    updatedGroup = new DynamoDBReplicationGroup(existingGroup);
                    updatedGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.UPDATING);
                    DynamoDBReplicationGroupMember updatedMember = updatedGroup.getReplicationGroupMembers().get(updateMemberRequest.getMemberArn());
                    updatedMember.setConnectors(updateMemberRequest.getConnectors());
                    updatedMember.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.UPDATING);

                    // try to write the updated group to the metadata table
                    errMessage = "error writing the replication group to the table " + updatedGroup;
                    if (updatedGroup.equals(md.compareAndWriteReplicationGroup(existingGroup, updatedGroup))) {
                        successBuilder.build();
                    } else {
                        LOGGER.error(errMessage);
                        return failedBuilder.encoding(errMessage).build();
                    }
                case ShutDownReplicationServerRequest:
                    DynamoDBReplicationServer.shutdownLatch.countDown();
                    return successBuilder.build();
                default:
                    errMessage = "Command not recognized by replication coordinator: " + request.getCommand();
                    LOGGER.error(errMessage);
                    return failedBuilder.entity(errMessage).build();
            }
        } catch (Exception e) {
            errMessage = "Exception while processing coordinator command: " + rawrequest + " with error: " + e;
            LOGGER.error(errMessage);
            return failedBuilder.entity(errMessage).build();
        }
    }

    /**
     * Checks the version number of the current replication group and returns the current replication group
     *
     * @param md
     *            the metadata storage access object
     * @param expectedVersion
     *            the expected version number of the current replication group
     * @param replicationGroupUUID
     *            the UUID of the replication group
     * @return the current replication group
     * @throws IOException
     *             error while writing to the metadata table
     */
    private DynamoDBReplicationGroup checkVersion(DynamoDBMetadataStorage md, long expectedVersion, String replicationGroupUUID) throws IOException {
        // if the expected version is not valid, return error
        if (expectedVersion <= 0) {
            return null;
        }

        // the expected version should match the version number of the current image of the replication group
        DynamoDBReplicationGroup curGroup = md.readReplicationGroup(replicationGroupUUID);
        if (curGroup == null) {
            return null;
        }
        if (curGroup.getVersion() != expectedVersion) {
            return null;
        }
        return curGroup;
    }

    /**
     * Sets the group status and all member statuses to DELETING
     *
     * @param curGroup
     *            the group to change the status for
     * @return the new group with updated statuses
     */
    private DynamoDBReplicationGroup getDeletingGroup(DynamoDBReplicationGroup curGroup) {
        DynamoDBReplicationGroup deletingGroup = new DynamoDBReplicationGroup(curGroup);
        deletingGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.DELETING);
        for (DynamoDBReplicationGroupMember member : deletingGroup.getReplicationGroupMembers().values()) {
            member.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.DELETING);
        }
        return deletingGroup;
    }

    /**
     * Sets the group status and all member statuses to CREATING
     *
     * @param curGroup
     *            the group to change the status for
     * @return the new group with updated statuses
     */
    private DynamoDBReplicationGroup getCreatingGroup(DynamoDBReplicationGroup curGroup) {
        DynamoDBReplicationGroup creatingGroup = new DynamoDBReplicationGroup(curGroup);
        creatingGroup.setReplicationGroupStatus(DynamoDBReplicationGroupStatus.CREATING);
        for (DynamoDBReplicationGroupMember member : creatingGroup.getReplicationGroupMembers().values()) {
            member.setReplicationGroupMemberStatus(DynamoDBReplicationGroupMemberStatus.CREATING);
        }
        return creatingGroup;
    }
}
