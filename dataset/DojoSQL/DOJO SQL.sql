CREATE TABLE Movies
(	
	ID INT IDENTITY(1,1) PRIMARY KEY,
	DATASET_ID INT NOT NULL,
	TITLE VARCHAR(255) NOT NULL
);

INSERT INTO Movies VALUES
(1,'Toy Story (1995)'),
(2,'Jumanji (1995)'),
(3,'Grumpier Old Men (1995)'),
(4,'Waiting to Exhale (1995)'),
(5,'Father of the Bride Part II (1995)'),
(6,'Heat (1995)'),
(7,'Sabrina (1995)'),
(8,'Tom and Huck (1995)'),
(9,'Sudden Death (1995)'),
(10,'GoldenEye (1995)');

CREATE TABLE Genres
(	
	ID INT IDENTITY(1,1) PRIMARY KEY,
	NAME VARCHAR(255) NOT NULL
);
INSERT INTO Genres VALUES
(1,'Adventure'),
(2,'Animation'),
(3,'Children'),
(4,'Comedy'),
(5,'Fantasy'),
(6,'Romance'),
(7,'Drama'),
(8,'Comedy'),
(9,'Action'),
(10,'Crime'),
(11,'Thriller');

CREATE TABLE Users
(	
	ID IDENTITY(1,1) PRIMARY KEY,
	NAME VARHCAR(255) NOT NULL,
	COPY_DATE DATE NOT NULL DEFAULT GetDate()
);
INSERT INTO Users VALUES
(1,'Ricardo'),
(2,'José'),
(3,'Marcos'),
(4,'Guilherme'),
(5,'Ana'),
(6,'Patricia'),
(7,'Roseli'),
(8,'Vitor'),
(9,'Alexandre'),
(10,'Juliano'),
(11,'Amanda');

DROP TABLE Copy;
CREATE TABLE Copy
(	
	ID IDENTITY(1,1) PRIMARY KEY,
	MOVIE_ID INT FOREIGN KEY REFERENCES Movies(ID)
);

INSERT INTO Copy VALUES
(1),
(1),
(2),
(2),
(4),
(4),
(5),
(5),
(6),
(6),
(7),
(8),
(9),
(10),
(10);

DROP TABLE Rent;
CREATE TABLE Rent
(	
	ID INT IDENTITY(1,1) PRIMARY KEY,
	COPY_ID INT FOREIGN KEY REFERENCES Copy(ID),
	USER_ID INT FOREIGN KEY REFERENCES Users(ID),
	RENT_DATE DATE NOT NULL,
	RETURN_DATE DATE NOT NULL,
	ACTUAL_RETURN_DATE DATE

);
INSERT INTO Rent VALUES
(1,'2021-01-20','2021-01-23','2021-01-23',1,1),
(2,'2021-01-20','2021-01-23','2021-01-24',2,1),
(3,'2021-01-10','2021-01-13','2021-01-14',3,2),
(4,'2021-02-14','2021-02-17','2021-02-16',4,3),
(5,'2021-02-19','2021-02-22','2021-02-22',5,4),
(6,'2021-03-11','2021-03-14','2021-03-14',6,5),
(7,'2021-03-20','2021-03-23','2021-03-24',7,6),
(8,'2021-04-16','2021-04-19','2021-04-18',8,7),
(9,'2021-04-12','2021-04-15','2021-04-16',9,8),
(10,'2021-05-22','2021-05-25','2021-05-24',10,9),
(11,'2021-05-30','2021-06-02','2021-06-01',4,10),
(12,'2021-06-10','2021-06-13','2021-06-15',6,11);

CREATE TABLE Genres_Link
(	
	MOVIE_ID INT FOREIGN KEY REFERENCES Movies(ID),
	GENRE_ID INT FOREIGN KEY REFERENCES Genres(ID)
);
INSERT INTO Genres_Link VALUES
(1,1),
(1,2),
(1,3),
(1,4),
(1,5),
(2,1),
(2,3),
(2,5),
(3,4),
(3,6),
(4,4),
(4,7),
(4,6),
(5,4),
(6,9),
(6,10),
(6,11),
(7,4),
(7,6),
(8,1),
(8,3),
(9,9),
(10,9),
(10,1),
(10,11);

CREATE TABLE Ratings
(	
	ID INT IDENTITY(1,1) PRIMARY KEY,
	MOVIE_ID INT FOREIGN KEY REFERENCES Movies(ID),
	VALUE INT NOT NULL
);
INSERT INTO Ratings VALUES
(1,1,4,'2021-01-23'),
(2,1,3,'2021-01-23'),
(3,2,5,'2021-01-14'),
(4,3,4,'2021-02-16'),
(5,4,2,'2021-02-22'),
(6,5,4,'2021-03-14'),
(7,6,3,'2021-03-24'),
(8,7,5,'2021-04-18'),
(9,8,3,'2021-04-16'),
(10,9,4,'2021-05-24'),
(4,10,4,'2021-06-01'),
(6,11,3,'2021-06-15');

---
CREATE PROCEDURE RENT_COPY @copyid int @userid int, @days int
AS
BEGIN
	DECLARE @Rented int
	SELECT @Rented = COUNT(1) FROM Rent WHERE COPY_ID = @copyid AND ACTUAL_RETURN_DATE IS NULL
	IF (@Rented = 1)
	BEGIN
		RAISEERROR ('Copy is already rented')
	END
	INSERT INTO Rent(COPY_ID, USER_ID, RENT_DATE, RETURN_DATE) VALUES(
		@copyid,
		@costumerid,
		GETDATE()
		DATEADD(DAY, @days, GETDATE()))
		)
END
EXECUTE RENT_COPY 2, 1, 3;

---
CREATE PROCEDURE RETURN_COPY @copyid INT
AS
	UPDATE Copy SET ACTUAL_RETURN_DATE = GETDATE() WHERE COPY_ID = @copyid AND ACTUAL_RETURN_DATE IS NULL;

EXECUTE RETURN_COPY 4;

---
CREATE OR ALTER VIEW DELAYED_RETURNS
AS
	SELECT CM.NAME, M.TITLE, CR.RETURN_DATE
		FROM COPY_RENTAL CR
		JOIN COPY C
			ON C.ID = CR.COPY_ID
		JOIN Users CM
			ON CM.ID = CR.USER_ID
		WHERE CR.ACTUAL_RETURN_DATE IS NULL
		AND CR.RETURN_DATE < GETDATE()
		
SELECT * FROM DELAYED_RETURNS

---
CREATE OR ALTER FUNCTION GET_AVAILABLE_COPIES(@movieid INT)
RETURNS TABLE
AS
RETURN
	SELECT * FROM Copy C WHERE C.MOVIE_ID = @movieid AND C.ID NOT IN(
		SELECT C2.ID
		FROM COPY C2
		JOIN COPY_RENTAL CR
			ON CR.COPY_ID = C2.ID
			AND C2.MOVIE_ID = @movieid
			AND CR.ACTUAL_RETURN_DATE IS NULL
		)
)

SELECT * FROM GET_AVAILABLE_COPIES(9743)

---




USE movies;

DROP PROCEDURE RentMovie;

CREATE PROCEDURE RentMovie (@id integer,
							@rent_date = GETDATE(),
							@expect_rent_return = DATEADD(day,3,GETDATE()),
							@movie_id integer,
							@user_id integer)
AS
  BEGIN
			
            INSERT INTO Rent(id,rent_date, expect_rent_return, actual_rent_return, movie_id,user_id)
            VALUES(
				@id,
				@rent_date,
				@expect_rent_return, 
				@copy_id,
				@user_id)
END

EXECUTE RentMovie 13,'2022-01-10','2022-01-13',',3,10;
