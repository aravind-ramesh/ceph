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

//Arav

void ECUtil::CrcInfoDiffs::print_crc_diffs()
{
  dout(1) << __func__ << " DBG: number of diffs = " << crc_diffs.size() << dendl;
  for (uint32_t i = 0; i < crc_diffs.size(); i++) {
    dout(1) << __func__ << " DBG: diff index " << i << dendl;
    uint32_t k = crc_diffs[i].stripelet_crc.size();
    dout(1) << __func__ << " DBG: number of crcs in stripelet crc " << k << dendl;
    for (uint32_t j = 0; j < k; j++) {
      dout(1) << __func__ << " DBG: crc " << j << " = " << crc_diffs[i].stripelet_crc[j] << dendl;
    } 
  }
}
void ECUtil::CrcInfoDiffs::append_crc(uint64_t old_size,
				      map<int, bufferlist> &to_append,
				      uint32_t shard,
				      uint32_t stripelet_size)
{
// Calculate stripelet crc for given data buffer.
// Check for any exisiting diff(one set of striplet crc), 
// if found, create a new diff and update the vector crc_diffs
// if not found, update the vector(push_back)

  dout(1) << __func__ << "shard: " << shard << dendl;
  dout(1) << __func__ << " stripelet_size: " << stripelet_size << dendl; 
  map<int, bufferlist>::iterator i = to_append.find(shard);
  assert(i != to_append.end()); // Hit the end of map, Could not find the expected shard-id, Assert.
  uint32_t p = 0;
  vector<uint32_t> s_crc(i->second.length()/stripelet_size, -1);
  for (uint32_t j = 0; j < i->second.length(); j += stripelet_size) {
    bufferlist buf;
    buf.substr_of(i->second, j, stripelet_size);
    uint32_t new_hash = buf.crc32c(-1);
    s_crc[p++] = new_hash;
  }
  ECUtil::CrcInfoDiffs::diff d;
  d.offset = old_size;
  //d.stripelet_crc = s_crc;
  d.stripelet_crc.reserve(d.stripelet_crc.size() + s_crc.size());
  d.stripelet_crc.insert(d.stripelet_crc.end(), s_crc.begin(), s_crc.end());
  //crc_diffs.push_back(d);
  crc_diffs.insert(crc_diffs.end(), d);
  //print_crc_diffs();
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

void ECUtil::CrcInfo::merge(const CrcInfoDiffs &shard_diffs, uint32_t stripelet_size)
{
  // Merge multiple diffs(or single diff) in to the shard_stripelet_crc vector
  dout(20) << __func__ << " shard_diffs.size: " << shard_diffs.crc_diffs.size() << dendl;
  CrcInfoDiffs::diff tmp_diff;
  uint32_t diffs_count;
  diffs_count = shard_diffs.crc_diffs.size();
  if (!diffs_count) {
    dout(20) << __func__ << " Asserting, diffs_count = 0" << dendl;
    assert(0);
  }
  for (uint32_t i = 0; i < diffs_count; i++) {
  // Process the diffs in crc_diffs vector and merge it to crcinfo.stripelet_size (which goes to disk).
    tmp_diff = shard_diffs.crc_diffs[i];
    if (!shard_stripelet_crc_v.size()) {
      // No crc's found in the vector, update the new vector
      shard_stripelet_crc_v = tmp_diff.stripelet_crc;
    } else {
      // There are some crcs already present, append/overwrite the vector
      shard_stripelet_crc_v.insert((shard_stripelet_crc_v.begin() + tmp_diff.offset/stripelet_size), tmp_diff.stripelet_crc.begin(), tmp_diff.stripelet_crc.end());
    }
    //TODO: Handle error condition where offset is pointing to a value beyond what the crc count indicates

    // TODO: Update total_shard_size based on stripelet_size*shard_stripelet_crc_v.size()
    total_shard_size += tmp_diff.stripelet_crc.size() * stripelet_size;
  }
}

void ECUtil::CrcInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_shard_size, bl);
  ::encode(shard_stripelet_crc_v, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::CrcInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_shard_size, bl);
  ::decode(shard_stripelet_crc_v, bl);
  DECODE_FINISH(bl);
}

void ECUtil::CrcInfo::dump(Formatter *f) const
{
  f->dump_unsigned("shard", shard);
  f->dump_unsigned("total_shard_size", total_shard_size);
  f->open_object_section("shard_stripelet_crc_v");
  for (unsigned i = 0; i != shard_stripelet_crc_v.size(); ++i) {
    f->open_object_section("hash");
    f->dump_unsigned("stripelet index ", i);
    f->dump_unsigned("hash", shard_stripelet_crc_v[i]);
    f->close_section();
  }
  f->close_section();
}
/*
void ECUtil::CrcInfoDiffs::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  __u32 n = crc_diffs.size();
  ::encode(n, bl);
  for (uint32_t i = 0; i < n; i++) {
    ::encode(crc_diffs[i].offset, bl);
    ::encode(crc_diffs[i].stripelet_crc, bl);
  }
  ENCODE_FINISH(bl);
}

void ECUtil::CrcInfoDiffs::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  __u32 n;
  ::decode(n, bl);
  for (uint32_t i = 0; i < n; i++) {
    ::decode(crc_diffs[i].offset, bl);
    ::decode(crc_diffs[i].stripelet_crc, bl);
  }
  DECODE_FINISH(bl);
}
*/
const string CINFO_KEY = "cinfo_key";

bool ECUtil::is_cinfo_key_string(const string &key)
{
  return key == CINFO_KEY;
}

const string &ECUtil::get_cinfo_key()
{
  return CINFO_KEY;
}
