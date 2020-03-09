// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// CAVEAT: This sample is to demonstrate azure IoT client concepts only and is not a guide design principles or style
// Checking of return codes and error values shall be omitted for brevity.  Please practice sound engineering practices
// when writing production code.

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "azure_c_shared_utility/threadapi.h"
#include "azure_c_shared_utility/shared_util_options.h"

#include "iothub.h"
#include "iothub_device_client.h"
#include "iothub_message.h"
#include "iothub_client_options.h"
#ifdef SET_TRUSTED_CERT_IN_SAMPLES
#include "certs.h"
#endif // SET_TRUSTED_CERT_IN_SAMPLES

#define SAMPLE_AMQP
//#define SAMPLE_AMQP_OVER_WEBSOCKETS
//#define SAMPLE_HTTP

#ifdef SAMPLE_AMQP
#include "iothubtransportamqp.h"
#endif // SAMPLE_AMQP
#ifdef SAMPLE_MQTT_OVER_WEBSOCKETS
#include "iothubtransportmqtt_websockets.h"
#endif // SAMPLE_MQTT_OVER_WEBSOCKETS
#ifdef SAMPLE_AMQP_OVER_WEBSOCKETS
#include "iothubtransportamqp_websockets.h"
#endif // SAMPLE_AMQP_OVER_WEBSOCKETS
#ifdef SAMPLE_HTTP
#include "iothubtransporthttp.h"
#endif // SAMPLE_HTTP

static const char* hubName = "iot-sdks-tcpstreaming";
static const char* hubSuffix = "azure-devices.net";


static const char** creds[] = {
    cred1,
    cred2,
    cred3,
    cred4
};

static IOTHUB_DEVICE_CLIENT_LL_HANDLE client_handles[4];

static bool g_continueRunning;
static char msgText[1024];
static char propText[1024];
#define MESSAGE_COUNT       50

typedef struct EVENT_INSTANCE_TAG
{
    size_t id;
    const char* device_id;
} EVENT_INSTANCE;

static IOTHUBMESSAGE_DISPOSITION_RESULT ReceiveMessageCallback(IOTHUB_MESSAGE_HANDLE message, void* userContextCallback)
{
    char* device_id = (char*)userContextCallback;
    const unsigned char* buffer = NULL;
    size_t size = 0;
    const char* messageId;
    const char* correlationId;

    // Message properties
    if ((messageId = IoTHubMessage_GetMessageId(message)) == NULL)
    {
        messageId = "<null>";
    }

    if ((correlationId = IoTHubMessage_GetCorrelationId(message)) == NULL)
    {
        correlationId = "<null>";
    }

    // Message content
    IOTHUBMESSAGE_CONTENT_TYPE contentType = IoTHubMessage_GetContentType(message);
    if (contentType == IOTHUBMESSAGE_BYTEARRAY)
    {
        if (IoTHubMessage_GetByteArray(message, &buffer, &size) == IOTHUB_MESSAGE_OK)
        {
            (void)printf("Received Message [%s]\r\n Message ID: %s\r\n Correlation ID: %s\r\n BINARY Data: <<<%.*s>>> & Size=%d\r\n", device_id, messageId, correlationId, (int)size, buffer, (int)size);
        }
        else
        {
            (void)printf("Failed getting the BINARY body of the message received.\r\n");
        }
    }
    else if (contentType == IOTHUBMESSAGE_STRING)
    {
        if ((buffer = (const unsigned char*)IoTHubMessage_GetString(message)) != NULL && (size = strlen((const char*)buffer)) > 0)
        {
            (void)printf("Received Message [%s]\r\n Message ID: %s\r\n Correlation ID: %s\r\n STRING Data: <<<%.*s>>> & Size=%d\r\n", device_id, messageId, correlationId, (int)size, buffer, (int)size);

            // If we receive the work 'quit' then we stop running
        }
        else
        {
            (void)printf("Failed getting the STRING body of the message received.\r\n");
        }
    }
    else
    {
        (void)printf("Failed getting the body of the message received (type %i).\r\n", contentType);
    }

    // Retrieve properties from the message
    const char* property_key = "property_key";
    const char* property_value = IoTHubMessage_GetProperty(message, property_key);
    if (property_value != NULL)
    {
        printf("\r\nMessage Properties:\r\n");
        printf("\tKey: %s Value: %s\r\n", property_key, property_value);
    }
    if (memcmp(buffer, "quit", size) == 0)
    {
        g_continueRunning = false;
    }
    /* Some device specific action code goes here... */
    return IOTHUBMESSAGE_ACCEPTED;
}

static void SendConfirmationCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void* userContextCallback)
{
    EVENT_INSTANCE* context = (EVENT_INSTANCE*)userContextCallback;

    (void)printf("SendConfirmationCallback (device_id=%s, msg_id=%lu, result=%s)\r\n", context->device_id, context->id, MU_ENUM_TO_STRING(IOTHUB_CLIENT_CONFIRMATION_RESULT, result));
    free(context);
}

static IOTHUB_MESSAGE_HANDLE create_event(const char* device_id, size_t id, EVENT_INSTANCE** context)
{
    IOTHUB_MESSAGE_HANDLE message_handle;

    if ((*context = malloc(sizeof(EVENT_INSTANCE))) == NULL)
    {
        message_handle = NULL;
    }
    else
    {
        if ((message_handle = IoTHubMessage_CreateFromString(device_id)) == NULL)
        {
            free(*context);
        }
        else
        {
            (*context)->device_id = device_id;
            (*context)->id = id;
        }
    }

    return message_handle;
}

static void connectionStatusCallback(IOTHUB_CLIENT_CONNECTION_STATUS result, IOTHUB_CLIENT_CONNECTION_STATUS_REASON reason, void* userContextCallback)
{
    const char* device_id = (const char*)userContextCallback;
    printf("connectionStatusCallback (device_id=%s, result=%s, reason=%s)\r\n", device_id, MU_ENUM_TO_STRING(IOTHUB_CLIENT_CONNECTION_STATUS, result), MU_ENUM_TO_STRING(IOTHUB_CLIENT_CONNECTION_STATUS_REASON, reason));
}


int main(void)
{
    TRANSPORT_HANDLE transport_handle;
    IOTHUB_CLIENT_TRANSPORT_PROVIDER protocol;

#ifdef SAMPLE_AMQP
    protocol = AMQP_Protocol;
#endif // SAMPLE_AMQP
#ifdef SAMPLE_AMQP_OVER_WEBSOCKETS
    protocol = AMQP_Protocol_over_WebSocketsTls;
#endif // SAMPLE_AMQP_OVER_WEBSOCKETS
#ifdef SAMPLE_HTTP
    protocol = HTTP_Protocol;
#endif // SAMPLE_HTTP

    g_continueRunning = true;

    //callbackCounter = 0;
    int receiveContext1 = 0;
    int receiveContext2 = 0;

    (void)printf("Starting the IoTHub client shared sample.  Send `quit` message to either device to close...\r\n");

    // Used to initialize IoTHub SDK subsystem
    (void)IoTHub_Init();
    
    if ((transport_handle = IoTHubTransport_Create(protocol, hubName, hubSuffix)) == NULL)
    {
        printf("Failed to creating the protocol handle.\r\n");
    }
    else
    {
        for (int i = 0; i < sizeof(creds)/sizeof(creds[0]); i++)
        {
            IOTHUB_CLIENT_DEVICE_CONFIG config = { 0 };
            config.deviceId = creds[i][0];
            config.deviceKey = creds[i][1];
            config.deviceSasToken = NULL;
            config.protocol = protocol;
            config.transportHandle = IoTHubTransport_GetLLTransport(transport_handle);


            if ((client_handles[i] = IoTHubDeviceClient_LL_CreateWithTransport(&config)) == NULL)
            {
                (void)printf("Failed creating iothub client handle %d\r\n", i);
            }
            else
            {
                IoTHubDeviceClient_LL_SetConnectionStatusCallback(client_handles[i], connectionStatusCallback, (void*)creds[i][0]);

                bool traceOn = true;
                IoTHubDeviceClient_LL_SetOption(client_handles[i], OPTION_LOG_TRACE, &traceOn);
                #ifdef SET_TRUSTED_CERT_IN_SAMPLES
                IoTHubDeviceClient_LL_SetOption(client_handles[i], OPTION_TRUSTED_CERT, certificates);
                #endif // SET_TRUSTED_CERT_IN_SAMPLES

                #ifdef SAMPLE_HTTP
                unsigned int timeout = 241000;
                // Because it can poll "after 9 seconds" polls will happen effectively // at ~10 seconds.
                // Note that for scalabilty, the default value of minimumPollingTime
                // is 25 minutes. For more information, see:
                // https://azure.microsoft.com/documentation/articles/iot-hub-devguide/#messaging
                unsigned int minimumPollingTime = 9;
                IoTHubDeviceClient_LL_SetOption(client_handles[i], OPTION_MIN_POLLING_TIME, &minimumPollingTime);
                IoTHubDeviceClient_LL_SetOption(client_handles[i], OPTION_HTTP_TIMEOUT, &timeout);
                #endif // SAMPLE_HTTP

                (void)IoTHubDeviceClient_LL_SetMessageCallback(client_handles[i], ReceiveMessageCallback, (void*)creds[i][0]);
            }
        }

        size_t messages_sent = 0;
        size_t counter = 0;
        do
        {
            if (counter % 100 == 0 && messages_sent < MESSAGE_COUNT)
            {
                for (int i = 0; i < sizeof(creds)/sizeof(creds[0]); i++)
                {
                    EVENT_INSTANCE* context;
                    IOTHUB_MESSAGE_HANDLE message_handle = create_event(creds[i][0], messages_sent, &context);
                    (void)IoTHubDeviceClient_LL_SendEventAsync(client_handles[i], message_handle, SendConfirmationCallback, context);
                    IoTHubMessage_Destroy(message_handle);
                }

                messages_sent++;
            }

            for (int i = 0; i < sizeof(creds)/sizeof(creds[0]); i++)
            {
                IoTHubDeviceClient_LL_DoWork(client_handles[i]);
            }

            ThreadAPI_Sleep(100);
            counter++;
        } while (g_continueRunning);

        for (int i = 0; i < sizeof(creds)/sizeof(creds[0]); i++)
        {
            IoTHubDeviceClient_LL_Destroy(client_handles[i]);
        }

        IoTHubTransport_Destroy(transport_handle);
        IoTHub_Deinit();
    }
    return 0;
}
