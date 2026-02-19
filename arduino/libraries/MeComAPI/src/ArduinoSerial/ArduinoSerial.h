#ifndef ARDUINOSERIAL_H
#define ARDUINOSERIAL_H

#ifdef ARDUINO
#include <Stream.h>

extern void ArduinoSerial_recvData(void);
extern void ArduinoSerial_Send(char* buffer);
extern Stream* ArduinoSerial;
#endif

#endif // !ARDUINOSERIAL_H