package com.outboundkafka.consumer;

public class OutboundDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		OutboundKafka outbound = new OutboundKafka("OutboundOffsetKafka");
		outbound.start();
	}

}
