# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: raft.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'raft.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nraft.proto\x12\x04raft\x1a\x1bgoogle/protobuf/empty.proto\"8\n\x12RequestVoteRequest\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x14\n\x0c\x63\x61ndidate_id\x18\x02 \x01(\t\"9\n\x13RequestVoteResponse\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x14\n\x0cvote_granted\x18\x02 \x01(\x08\"7\n\x14\x41ppendEntriesRequest\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x11\n\tleader_id\x18\x02 \x01(\t\"6\n\x15\x41ppendEntriesResponse\x12\x0c\n\x04term\x18\x01 \x01(\x05\x12\x0f\n\x07success\x18\x02 \x01(\x08\"\'\n\x12LeaderNotification\x12\x11\n\tleader_id\x18\x01 \x01(\t\"(\n\nPutRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\"\x1e\n\x0bPutResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\".\n\x10ReplicateRequest\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t\" \n\x11ReplicateResponse\x12\x0b\n\x03\x61\x63k\x18\x01 \x01(\x08\"4\n\x0eLeaderResponse\x12\x11\n\tleader_id\x18\x01 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x02 \x01(\t2\xfd\x02\n\x04Raft\x12\x42\n\x0bRequestVote\x12\x18.raft.RequestVoteRequest\x1a\x19.raft.RequestVoteResponse\x12H\n\rAppendEntries\x12\x1a.raft.AppendEntriesRequest\x1a\x1b.raft.AppendEntriesResponse\x12@\n\x0cNotifyLeader\x12\x18.raft.LeaderNotification\x1a\x16.google.protobuf.Empty\x12*\n\x03Put\x12\x10.raft.PutRequest\x1a\x11.raft.PutResponse\x12<\n\tReplicate\x12\x16.raft.ReplicateRequest\x1a\x17.raft.ReplicateResponse\x12;\n\x0bWhoIsLeader\x12\x16.google.protobuf.Empty\x1a\x14.raft.LeaderResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'raft_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_REQUESTVOTEREQUEST']._serialized_start=49
  _globals['_REQUESTVOTEREQUEST']._serialized_end=105
  _globals['_REQUESTVOTERESPONSE']._serialized_start=107
  _globals['_REQUESTVOTERESPONSE']._serialized_end=164
  _globals['_APPENDENTRIESREQUEST']._serialized_start=166
  _globals['_APPENDENTRIESREQUEST']._serialized_end=221
  _globals['_APPENDENTRIESRESPONSE']._serialized_start=223
  _globals['_APPENDENTRIESRESPONSE']._serialized_end=277
  _globals['_LEADERNOTIFICATION']._serialized_start=279
  _globals['_LEADERNOTIFICATION']._serialized_end=318
  _globals['_PUTREQUEST']._serialized_start=320
  _globals['_PUTREQUEST']._serialized_end=360
  _globals['_PUTRESPONSE']._serialized_start=362
  _globals['_PUTRESPONSE']._serialized_end=392
  _globals['_REPLICATEREQUEST']._serialized_start=394
  _globals['_REPLICATEREQUEST']._serialized_end=440
  _globals['_REPLICATERESPONSE']._serialized_start=442
  _globals['_REPLICATERESPONSE']._serialized_end=474
  _globals['_LEADERRESPONSE']._serialized_start=476
  _globals['_LEADERRESPONSE']._serialized_end=528
  _globals['_RAFT']._serialized_start=531
  _globals['_RAFT']._serialized_end=912
# @@protoc_insertion_point(module_scope)
