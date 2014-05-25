from lshash import LSHash
lsh = LSHash(hash_size=6, input_dim=8, num_hashtables=1, storage_config={"lmdb": {'path': '/Users/christianburger/Downloads/testlmdb'}})
lsh.index([1,2,3,4,5,6,7,8], 'a')
lsh.index([2,3,4,5,6,7,8,9], 'b')
lsh.index([10,12,99,1,5,31,2,3], 'c')
print lsh.query([1,2,3,4,5,6,7,7])