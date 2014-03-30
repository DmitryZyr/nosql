using System;
using System.Reflection;
using CorrugatedIron;
using CorrugatedIron.Models;
using Tweets.Attributes;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly string bucketName;
        private readonly IRiakClient riakClient;
        private readonly IMapper<User, UserDocument> userDocumentMapper;
        private readonly IMapper<UserDocument, User> userMapper;

        public UserRepository(IRiakClient riakClient, IMapper<User, UserDocument> userDocumentMapper, IMapper<UserDocument, User> userMapper)
        {
            this.riakClient = riakClient;
            this.userDocumentMapper = userDocumentMapper;
            this.userMapper = userMapper;
            bucketName = typeof (UserDocument).GetCustomAttribute<BucketNameAttribute>().BucketName;
        }

        public void Save(User user)
        {
            var userDocument = userDocumentMapper.Map(user);
            var result = riakClient.Put(new RiakObject(bucketName, userDocument.Id, userDocument));
            CheckResult(result);
        }

        public User Get(string userId)
        {
            var result = riakClient.Get(bucketName, userId);
            CheckResult(result);
            var userDocument = result.Value.GetObject<UserDocument>();
            if (userDocument == null)
                return null;
            return userMapper.Map(userDocument);
        }

        private void CheckResult(RiakResult<RiakObject> result)
        {
            if (!result.IsSuccess)
                throw new Exception(string.Format("Riak client throws exception: {0} code: {1}", result.ErrorMessage, result.ResultCode));
        }
    }
}