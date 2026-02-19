#ifdef ARDUINO
#include "../MeComAPI/MePort.h"
#include "ArduinoSerial.h"
#include <Arduino.h>

#define RS485_MODE_PIN 4

#define RS485_RECEIVE 0
#define RS485_TRANSMIT 1

void setRS485Mode(int mode) {
    switch (mode) {
    case RS485_RECEIVE:
        digitalWrite(RS485_MODE_PIN, LOW);
        break;
    case RS485_TRANSMIT:
        digitalWrite(RS485_MODE_PIN, HIGH);
        break;
    }
}


void ArduinoSerial_recvData(void)
{
    char RcvBuf[MEPORT_MAX_RX_BUF_SIZE + 1];
    unsigned long startTime = millis();
    size_t nread = 0;
    do
    {
        int in = ArduinoSerial->read();
        if (in < 0) continue;
        RcvBuf[nread] = in;
        bool eol = in == '\r';
        nread++;
        if (eol || nread >= MEPORT_MAX_RX_BUF_SIZE) break;
    } while (millis() - startTime < MEPORT_SET_AND_QUERY_TIMEOUT);

    if (nread > 0) {
        RcvBuf[nread] = 0;
        MePort_ReceiveByte((int8_t*)RcvBuf);
    }
}

void ArduinoSerial_Send(char* buffer)
{
    setRS485Mode(RS485_TRANSMIT);
    ArduinoSerial->write(buffer);
    ArduinoSerial->flush();
    setRS485Mode(RS485_RECEIVE);
}

#endif
