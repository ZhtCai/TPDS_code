#include <stdio.h>
#include <tchar.h>
#include <string>
#include <iostream>
#include <ctime>
#include <secp256k1.h>
#include <zmq.hpp>
#include <json/json.h>
#include "sgx_urts.h"
#include "vEnclave_u.h"

using namespace std;

#define ENCLAVE_FILE _T("vEnclave.signed.dll")
#define MAX_BUF_LEN 5000


void ocall_sprintf(unsigned char* msg, char* h, int length)
{
	char buf[3];
	for (int i = 0; i < length; i++) {
		sprintf_s(buf, "%02x", msg[i]);
		strcat(h, buf);
	}
	cout << endl;
	//cout << h << endl;
}

void ocall_print(const char *str)
{
	printf("%s\n", str);
}

int main()
{
	sgx_enclave_id_t eid;
	sgx_status_t ret = SGX_SUCCESS;
	sgx_launch_token_t token = { 0 };
	int updated = 0;
	string buffer = "hello world";
	char h[MAX_BUF_LEN];

	zmq::context_t context(1);
	zmq::socket_t socket(context, ZMQ_REP);
	socket.bind("tcp://*:20021");

	// Create the Enclave with above launch token.
	ret = sgx_create_enclave(ENCLAVE_FILE, SGX_DEBUG_FLAG, &token, &updated, &eid, NULL);
	if (ret != SGX_SUCCESS) {
		printf("App: error %#x, failed to create enclave.\n", ret);
		return -1;
	}

	while (true) {
		zmq::message_t request;
		socket.recv(&request);
		Json::Reader reader;
		Json::Value root;
		string block_data = "";
		if (reader.parse(request.to_string(), root)) {
			block_data += root["shard_id"].asString();
			block_data += root["shard_length"].asString();
			block_data += root["timestamp"].asString();
			string temp = root["merkle"].asString();
			if (temp != "None")
				block_data += temp;
			
			temp = root["txmerkle"].asString();
			if (temp != "None")
				block_data += temp;

			block_data += root["nonce"].asString();
			cout << block_data.c_str() << endl;
		}
		// An Enclave call (ECALL) will happen here.
		ecall_verify_txblock(eid, NULL, block_data.c_str(), h, block_data.size());
		//cout << h << endl;
		socket.send(zmq::buffer("ok"));
	}
	//clock_t s = clock();
	
	//cout << h << endl;
	//cout << clock() - s << endl;
	// Destroy the enclave when all Enclave calls finished.
	if (SGX_SUCCESS != sgx_destroy_enclave(eid))
		return -1;
	return 0;
}