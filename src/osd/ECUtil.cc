// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"
#include "ECTransaction.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)
static ostream& _prefix(std::ostream *_dout)
{
  return *_dout << "ECUtil:";
}

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out) {
  assert(to_decode.size());

  uint64_t total_data_size = to_decode.begin()->second.length();
  assert(total_data_size % sinfo.get_chunk_size() == 0);

  assert(out);
  assert(out->length() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_data_size);
  }

  if (total_data_size == 0)
    return 0;

  for (uint64_t i = 0; i < total_data_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    bufferlist bl;
    int r = ec_impl->decode_concat(chunks, &bl);
    assert(bl.length() == sinfo.get_stripe_width());
    assert(r == 0);
    out->claim_append(bl);
  }
  return 0;
}

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out) {
  assert(to_decode.size());

  uint64_t total_data_size = to_decode.begin()->second.length();
  assert(total_data_size % sinfo.get_chunk_size() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_data_size);
  }

  if (total_data_size == 0)
    return 0;

  set<int> need;
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second);
    assert(i->second->length() == 0);
    need.insert(i->first);
  }

  for (uint64_t i = 0; i < total_data_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    map<int, bufferlist> out_bls;
    int r = ec_impl->decode(need, chunks, &out_bls);
    assert(r == 0);
    for (map<int, bufferlist*>::iterator j = out.begin();
	 j != out.end();
	 ++j) {
      assert(out_bls.count(j->first));
      assert(out_bls[j->first].length() == sinfo.get_chunk_size());
      j->second->claim_append(out_bls[j->first]);
    }
  }
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second->length() == total_data_size);
  }
  return 0;
}

int ECUtil::encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out) {

  uint64_t logical_size = in.length();

  assert(logical_size % sinfo.get_stripe_width() == 0);
  assert(out);
  assert(out->empty());

  if (logical_size == 0)
    return 0;

  for (uint64_t i = 0; i < logical_size; i += sinfo.get_stripe_width()) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, sinfo.get_stripe_width());
    int r = ec_impl->encode(want, buf, &encoded);
    assert(r == 0);
    for (map<int, bufferlist>::iterator i = encoded.begin();
	 i != encoded.end();
	 ++i) {
      assert(i->second.length() == sinfo.get_chunk_size());
      (*out)[i->first].claim_append(i->second);
    }
  }

  for (map<int, bufferlist>::iterator i = out->begin();
       i != out->end();
       ++i) {
    assert(i->second.length() % sinfo.get_chunk_size() == 0);
    assert(
      sinfo.aligned_chunk_offset_to_logical_offset(i->second.length()) ==
      logical_size);
  }
  return 0;
}

void ECUtil::HashInfo::append(uint64_t old_size,
			      map<int, bufferlist> &to_append) {
  assert(to_append.size() == cumulative_shard_hashes.size());
  assert(old_size == total_chunk_size);
  uint64_t size_to_append = to_append.begin()->second.length();
  for (map<int, bufferlist>::iterator i = to_append.begin();
       i != to_append.end();
       ++i) {
    assert(size_to_append == i->second.length());
    assert((unsigned)i->first < cumulative_shard_hashes.size());
    uint32_t new_hash = i->second.crc32c(cumulative_shard_hashes[i->first]);
    cumulative_shard_hashes[i->first] = new_hash;
  }
  total_chunk_size += size_to_append;
}

void ECUtil::HashInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_chunk_size, bl);
  ::encode(cumulative_shard_hashes, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::HashInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_chunk_size, bl);
  ::decode(cumulative_shard_hashes, bl);
  DECODE_FINISH(bl);
}

void ECUtil::HashInfo::dump(Formatter *f) const
{
  f->dump_unsigned("total_chunk_size", total_chunk_size);
  f->open_object_section("cumulative_shard_hashes");
  for (unsigned i = 0; i != cumulative_shard_hashes.size(); ++i) {
    f->open_object_section("hash");
    f->dump_unsigned("shard", i);
    f->dump_unsigned("hash", cumulative_shard_hashes[i]);
    f->close_section();
  }
  f->close_section();
}

void ECUtil::HashInfo::generate_test_instances(list<HashInfo*>& o)
{
  o.push_back(new HashInfo(3));
  {
    bufferlist bl;
    bl.append_zero(20);
    map<int, bufferlist> buffers;
    buffers[0] = bl;
    buffers[1] = bl;
    buffers[2] = bl;
    o.back()->append(0, buffers);
    o.back()->append(20, buffers);
  }
  o.push_back(new HashInfo(4));
}

const string HINFO_KEY = "hinfo_key";

bool ECUtil::is_hinfo_key_string(const string &key)
{
  return key == HINFO_KEY;
}

const string &ECUtil::get_hinfo_key()
{
  return HINFO_KEY;
}

void ECUtil::CrcInfoDiffs::append_crc(uint64_t old_size,
                                      bufferlist &bl,
                                      uint32_t stripelet_size)
{

  dout(1) << __func__ << " stripelet_size: " << stripelet_size << dendl;
  uint32_t p = 0;
  vector<uint32_t> s_crc(bl.length()/stripelet_size, -1);
  for (uint32_t j = 0; j < bl.length(); j += stripelet_size) {
    bufferlist buf;
    buf.substr_of(bl, j, stripelet_size);
    uint32_t new_hash = buf.crc32c(-1);
    s_crc[p++] = new_hash;
  }
  ECUtil::CrcInfoDiffs::diff d;
  d.offset = old_size;
  d.stripelet_crc.reserve(d.stripelet_crc.size() + s_crc.size());
  d.stripelet_crc.insert(d.stripelet_crc.end(), s_crc.begin(), s_crc.end());
  crc_diffs.insert(crc_diffs.end(), d);
}

void ECUtil::CrcInfoDiffs::dump(Formatter *f) const
{
  f->open_object_section("crc diffs");
  for (unsigned i = 0; i != crc_diffs.size(); ++i) {
    f->open_object_section("diffs");
    f->dump_unsigned("diff index ", i);
    f->dump_unsigned(" Offset ", crc_diffs[i].offset);
    for (unsigned j = 0; j < crc_diffs[i].stripelet_crc.size(); j++) {
      f->dump_unsigned(" stripelet hash ",crc_diffs[i].stripelet_crc[j]);
    }
  }
  f->close_section();
}

void ECUtil::CrcInfo::update_crc(vector<uint32_t> crc_v,
                                 uint32_t crc_insert_index)
{
  dout(25) << __func__ << " DBG : size of crc_v = " << crc_v.size() <<
                " crc_insert_index " << crc_insert_index <<
                " shard_stripelet_crc_v.size() " << shard_stripelet_crc_v.size()
                << dendl;
  if ((crc_insert_index + crc_v.size()) <= shard_stripelet_crc_v.size()) {
    copy(crc_v.begin(), crc_v.end(), (shard_stripelet_crc_v.begin() +
                                              crc_insert_index));
    dout(25) << " DBG shard_stripelet_crc_v.size() "
			    << shard_stripelet_crc_v.size() << dendl;
  } else {
    shard_stripelet_crc_v.resize(crc_insert_index + crc_v.size());
    copy(crc_v.begin(), crc_v.end(), (shard_stripelet_crc_v.begin() +
                                              crc_insert_index));
    dout(25) << " DBG ow/append - shard_stripelet_crc_v.size() "
			  << shard_stripelet_crc_v.size() << dendl;
  }
}

bool ECUtil::CrcInfo::verify_stripelet_crc(uint64_t offset, bufferlist bl,
                                            uint32_t stripelet_size,
                                            uint32_t crc_index,
                                            uint32_t crcs_to_verify)
{
  bool r = false;
  for (uint32_t j = offset; j < bl.length() && crcs_to_verify;
          j += stripelet_size) {
    bufferlist buf;
    buf.substr_of(bl, j, stripelet_size);
    uint32_t new_hash = buf.crc32c(-1);
    if (new_hash != shard_stripelet_crc_v[crc_index]) {
      dout(1) << "DBG CRC Mismatch" << "Calculated crc= " << new_hash
        << "Expected crc = " << shard_stripelet_crc_v[crc_index] << dendl;
      return false;
    }
    crc_index++;
    crcs_to_verify--;
    r = true;
  }
  return r;
}

void ECUtil::CrcInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(shard_stripelet_crc_v, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::CrcInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(shard_stripelet_crc_v, bl);
  DECODE_FINISH(bl);
}

void ECUtil::CrcInfo::dump(Formatter *f) const
{
  f->open_object_section("shard_stripelet_crc_v");
  for (unsigned i = 0; i != shard_stripelet_crc_v.size(); ++i) {
    f->open_object_section("hash");
    f->dump_unsigned("stripelet index ", i);
    f->dump_unsigned("crc", shard_stripelet_crc_v[i]);
    f->close_section();
  }
  f->close_section();
}
const string CINFO_KEY = "cinfo_key";

bool ECUtil::is_cinfo_key_string(const string &key)
{
  return key == CINFO_KEY;
}

const string &ECUtil::get_cinfo_key()
{
  return CINFO_KEY;
}
