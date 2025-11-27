#include "genericInput.h"

GenericInput::~GenericInput() {}
void GenericInput::initialize() {}
double GenericInput::getPacket(can_frame * const frame, std::atomic<bool>& EOI){}
