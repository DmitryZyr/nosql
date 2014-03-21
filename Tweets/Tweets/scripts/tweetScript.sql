CREATE TABLE "messages" (
	"id" UNIQUEIDENTIFIER NOT NULL,
	"userName" VARCHAR(100) NULL,
	"text" VARCHAR(1000) NULL,
	"createDate" DATETIME NULL,
	"version" ROWVERSION NOT NULL,
	PRIMARY KEY ("id")
);

CREATE TABLE "likes" (
	"userName" VARCHAR(100) NOT NULL,
	"messageId" UNIQUEIDENTIFIER NOT NULL,
	"createDate" DATETIME NULL,
	PRIMARY KEY ("userName", "messageId"),
	FOREIGN KEY (messageId) REFERENCES messages(id)
);