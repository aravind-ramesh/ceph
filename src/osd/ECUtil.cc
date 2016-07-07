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
void ECUtil::HashInfo::append_stripelet_crc(uint64_t old_size,
                                        map<int, bufferlist> &to_append,
                                        uint32_t stripewidth,
                                        int datachunk_count) {
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
    uint32_t stripelet_size = stripewidth/datachunk_count;
    uint32_t p = 0;
    vector<uint32_t> stripelet_crc(i->second.length()/stripelet_size, -1);
    for (uint32_t j = 0; j < i->second.length(); j += stripelet_size) {
        bufferlist buf;
        buf.substr_of(i->second, j, stripelet_size);

        uint32_t new_hash = buf.crc32c(-1);
        stripelet_crc[p++] = new_hash;
    }
        cumulative_stripelet_hashes[i->first] = stripelet_crc;
  }
  total_chunk_size += size_to_append;
}

bool ECUtil::HashInfo::verify_stripelet_crc(int shard, bufferlist bl, int stripelet_size)
{
  dout(15) << __func__ << dendl;

  for (uint32_t j = 0, i = 0; j < bl.length(); j += stripelet_size, i++) {
       bufferlist buf;
       buf.substr_of(bl, j, stripelet_size);
       uint32_t new_hash = buf.crc32c(-1);
       if (new_hash != cumulative_stripelet_hashes[shard][i]) {
          // CRC mismatch for this stripelet, return false
          dout(1) << "CRC Mismatch for shard " << shard << "Calculated crc = " << buf.crc32c(-1) << "Expected crc = " << cumulative_stripelet_hashes[shard][i] << dendl;
          return false;
       }
          dout(20) << "Calculated crc = " << buf.crc32c(-1) << " Expected crc =" << cumulative_stripelet_hashes[shard][i] << dendl;
  }

  return true;
}

void ECUtil::HashInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_chunk_size, bl);
  __u32 n = cumulative_stripelet_hashes.size();
  ::encode(n, bl);
  for (map<int, vector<uint32_t>>::iterator i = cumulative_stripelet_hashes.begin();
       i != cumulative_stripelet_hashes.end(); ++i) {
        int index = i->first;
        ::encode(index, bl);
        ::encode(i->second, bl);
  }
  ::encode(cumulative_shard_hashes, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::HashInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_chunk_size, bl);
  __u32 n;
  ::decode(n, bl);
  cumulative_stripelet_hashes.clear();
  while(n--) {
  int index;
  ::decode(index, bl);
  ::decode(cumulative_stripelet_hashes[index], bl);
  }
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
