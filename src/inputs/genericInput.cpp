#include "genericInput.h"

GenericInput::~GenericInput() {}
void GenericInput::initialize(bool adjust_timestamp) {}
double GenericInput::getPacket(can_frame * const frame, std::atomic<bool>& EOI){}
