/**
 * Copyright (c) 2019. Loïc Pottier.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */
#ifndef MY_CONFIG_H
#define MY_CONFIG_H

#include <wrench-dev.h>

namespace config {
    /* Payload used for all storages. The default payloads are 1024 for every events */
    std::map<std::string, double> message_payload_values = {
        {wrench::SimpleStorageServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD,              0},
        {wrench::SimpleStorageServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD,           0},
        {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_REQUEST_MESSAGE_PAYLOAD,       0},
        {wrench::SimpleStorageServiceMessagePayload::FREE_SPACE_ANSWER_MESSAGE_PAYLOAD,        0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_REQUEST_MESSAGE_PAYLOAD,      0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_DELETE_ANSWER_MESSAGE_PAYLOAD,       0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_REQUEST_MESSAGE_PAYLOAD,      0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_LOOKUP_ANSWER_MESSAGE_PAYLOAD,       0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_REQUEST_MESSAGE_PAYLOAD,        0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_COPY_ANSWER_MESSAGE_PAYLOAD,         0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_REQUEST_MESSAGE_PAYLOAD,       0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_WRITE_ANSWER_MESSAGE_PAYLOAD,        0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_READ_REQUEST_MESSAGE_PAYLOAD,        0},
        {wrench::SimpleStorageServiceMessagePayload::FILE_READ_ANSWER_MESSAGE_PAYLOAD,         0},
    };

    /* Payload used for all compute services */
    std::map<std::string, double> compute_payload_values = {
        {wrench::ComputeServiceMessagePayload::JOB_TYPE_NOT_SUPPORTED_MESSAGE_PAYLOAD,         0},
        {wrench::ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD,    32000000000},
        {wrench::ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,     0},
        {wrench::ComputeServiceMessagePayload::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,              0},
        {wrench::ComputeServiceMessagePayload::STANDARD_JOB_FAILED_MESSAGE_PAYLOAD,            0},
        {wrench::ComputeServiceMessagePayload::TERMINATE_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, 0},
        {wrench::ComputeServiceMessagePayload::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  0},
        {wrench::ComputeServiceMessagePayload::SUBMIT_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,       0},
        {wrench::ComputeServiceMessagePayload::SUBMIT_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,        0},
        {wrench::ComputeServiceMessagePayload::PILOT_JOB_STARTED_MESSAGE_PAYLOAD,              0},
        {wrench::ComputeServiceMessagePayload::PILOT_JOB_EXPIRED_MESSAGE_PAYLOAD,              0},
        {wrench::ComputeServiceMessagePayload::PILOT_JOB_FAILED_MESSAGE_PAYLOAD,               0},
        {wrench::ComputeServiceMessagePayload::TTL_REQUEST_MESSAGE_PAYLOAD,                    0},
        {wrench::ComputeServiceMessagePayload::TTL_ANSWER_MESSAGE_PAYLOAD,                     0},
        {wrench::ComputeServiceMessagePayload::TERMINATE_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,    0},
        {wrench::ComputeServiceMessagePayload::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,     0},
        {wrench::ComputeServiceMessagePayload::RESOURCE_DESCRIPTION_REQUEST_MESSAGE_PAYLOAD,   0},
        {wrench::ComputeServiceMessagePayload::RESOURCE_DESCRIPTION_ANSWER_MESSAGE_PAYLOAD,    0},
    };
}

#endif //MY_CONFIG_H
