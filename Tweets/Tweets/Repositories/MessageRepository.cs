using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Linq;
using System.Linq;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class MessageRepository : IMessageRepository
    {
        private readonly IMapper<Message, MessageDocument> messageDocumentMapper;
        private readonly Table<LikeDocument> likes;
        private readonly Table<MessageDocument> messages;
        private readonly DataContext dataContext;

        public MessageRepository(IMapper<Message, MessageDocument> messageDocumentMapper)
        {
            this.messageDocumentMapper = messageDocumentMapper;
            var connectionString = ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
            dataContext = new DataContext(connectionString);
            likes = dataContext.GetTable<LikeDocument>();
            messages = dataContext.GetTable<MessageDocument>();
        }

        public void Save(Message message)
        {
            var messageDocument = messageDocumentMapper.Map(message);
            messages.InsertOnSubmit(messageDocument);
            dataContext.SubmitChanges();
        }

        public void Like(Guid messageId, User user)
        {
            var likeDocument = new LikeDocument { MessageId = messageId, UserName = user.Name, CreateDate = DateTime.UtcNow };
            likes.InsertOnSubmit(likeDocument);
            dataContext.SubmitChanges();
        }

        public void Dislike(Guid messageId, User user)
        {
            var userLikes = likes.Where(x => x.MessageId == messageId && x.UserName == user.Name);
            likes.DeleteAllOnSubmit(userLikes);
            dataContext.SubmitChanges();
        }

        public IEnumerable<Message> GetPopularMessages()
        {
            return messages.GroupJoin(likes, x => x.Id, y => y.MessageId, (m, l) => new {MessageInfo = m, LikeInfo = l})
                           .OrderByDescending(x => x.LikeInfo.Count())
                           .Take(10)
                           .Select(x => new Message
                               {
                                   Id = x.MessageInfo.Id,
                                   CreateDate = x.MessageInfo.CreateDate,
                                   Text = x.MessageInfo.Text,
                                   User = new User {Name = x.MessageInfo.UserName},
                                   Likes = x.LikeInfo.Count()
                               })
                           .ToArray();
        }

        public IEnumerable<UserMessage> GetMessages(User user)
        {
            return messages.Where(x => x.UserName == user.Name)
                           .GroupJoin(likes, x => x.Id, y => y.MessageId, (m, l) => new {MessageInfo = m, LikeInfo = l})
                           .Select(x => new UserMessage
                               {
                                   Id = x.MessageInfo.Id,
                                   CreateDate = x.MessageInfo.CreateDate,
                                   Text = x.MessageInfo.Text,
                                   User = user,
                                   Likes = x.LikeInfo.Count(),
                                   Liked = x.LikeInfo.Any(y => y.UserName == user.Name)
                               })
                           .OrderByDescending(x => x.CreateDate);
        }
    }
}