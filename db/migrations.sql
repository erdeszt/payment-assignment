CREATE SCHEMA `challenge`;
USE `challenge`;

DROP TABLE IF EXISTS `invoice`;
CREATE TABLE `invoice` (
  `invoiceId` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `total` double NOT NULL,
  `payerId` int(10) unsigned NOT NULL,
  `sentAt` datetime NOT NULL,
  PRIMARY KEY (`invoiceId`)
);

DROP TABLE IF EXISTS `payer`;
CREATE TABLE `payer` (
  `payerId` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`payerId`)
);

DROP TABLE IF EXISTS `payment`;
CREATE TABLE `payment` (
  `paymentId` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `amount` double unsigned NOT NULL,
  `payerId` int(10) unsigned NOT NULL,
  `receivedAt` datetime NOT NULL,
  PRIMARY KEY (`paymentId`)
);
