using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class MessageRepository : IMessageRepository
    {
        private readonly IMapper<Message, MessageDocument> messageDocumentMapper;
        private readonly MongoCollection<MessageDocument> messagesCollection;

        public MessageRepository(IMapper<Message, MessageDocument> messageDocumentMapper)
        {
            this.messageDocumentMapper = messageDocumentMapper;
            var connectionString = ConfigurationManager.ConnectionStrings["MongoDb"].ConnectionString;
            var databaseName = MongoUrl.Create(connectionString).DatabaseName;
            messagesCollection =
                new MongoClient(connectionString).GetServer().GetDatabase(databaseName).GetCollection<MessageDocument>(MessageDocument.CollectionName);
        }

        public void Save(Message message)
        {
            var messageDocument = messageDocumentMapper.Map(message);
            messagesCollection.Insert(messageDocument);
        }

        public void Like(Guid messageId, User user)
        {
            var likeDocument = new LikeDocument {UserName = user.Name, CreateDate = DateTime.UtcNow};
            var addLike = Update<MessageDocument>.AddToSet(e => e.Likes, likeDocument);
            var likeNotExsist = Query.Not(Query<MessageDocument>.ElemMatch(x => x.Likes, x => x.EQ(y => y.UserName, user.Name)));
            messagesCollection.Update(Query.And(GetMessageById(messageId), likeNotExsist), addLike);
        }

        public void Dislike(Guid messageId, User user)
        {
            var removeLike = Update<MessageDocument>.Pull(e => e.Likes, x => x.EQ(e => e.UserName, user.Name));
            messagesCollection.Update(GetMessageById(messageId), removeLike);
        }

        private IMongoQuery GetMessageById(Guid messageId)
        {
            return Query<MessageDocument>.EQ(e => e.Id, messageId);
        }

        public IEnumerable<Message> GetPopularMessages()
        {
            var project = new BsonDocument("$project", new BsonDocument
                {
                    {"likes", new BsonDocument("$ifNull", 
                        new BsonArray(new BsonValue[] {"$likes", new BsonArray {BsonNull.Value}}))},
                    {"text", 1},
                    {"createDate", 1},
                    {"userName", 1},
                });
            var unwind = new BsonDocument("$unwind", "$likes");
            var orderBy = new BsonDocument("$sort", new BsonDocument("countLikes", -1));
            var limit = new BsonDocument("$limit", 10);
            var groupBy = new BsonDocument("$group", new BsonDocument
                {
                    {"countLikes", new BsonDocument("$sum", new BsonDocument("$cond", 
                        new BsonArray {new BsonDocument("$eq", new BsonArray {"$likes", BsonNull.Value}), 0, 1}))},
                    {
                        "_id", new BsonDocument
                            {
                                {"_id", "$_id"},
                                {"userName", "$userName"},
                                {"text", "$text"},
                                {"createDate", "$createDate"},
                            }
                    }
                });

            return messagesCollection.Aggregate(project, unwind, groupBy, orderBy, limit).ResultDocuments.Select(x =>
                {
                    var message = BsonSerializer.Deserialize<MessageDocument>((BsonDocument) x["_id"]);
                    var countLikes = (int) x["countLikes"];
                    return new Message
                        {
                            Id = message.Id,
                            Likes = countLikes,
                            CreateDate = message.CreateDate,
                            Text = message.Text,
                            User = new User {Name = message.UserName}
                        };
                });
        }

        public IEnumerable<UserMessage> GetMessages(User user)
        {
            var query = Query<MessageDocument>.EQ(x => x.UserName, user.Name);
            return messagesCollection.Find(query).Select(x => new UserMessage
                {
                    Id = x.Id,
                    CreateDate = x.CreateDate,
                    Text = x.Text,
                    User = user,
                    Likes = x.Likes == null ? 0 : x.Likes.Count(),
                    Liked = x.Likes != null && x.Likes.Any(y => y.UserName == user.Name)
                }).OrderByDescending(x => x.CreateDate);
        }
    }
}