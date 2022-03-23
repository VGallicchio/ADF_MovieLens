--Procedure para alugar uma respectiva cópia de um filme, caso ela não esteja alugada, se estiver não permite a locação.
CREATE PROCEDURE RENT_COPY @copyid INT, @userid INT, @days INT
AS
BEGIN
	DECLARE @Rented int
	SELECT @Rented = COUNT(1) FROM Rent WHERE COPY_ID = @copyid AND ACTUAL_RETURN_DATE IS NULL
	IF (@Rented = 1)
	BEGIN
		RAISERROR ('Copy is already rented', 16,1)
		RETURN
	END
	INSERT INTO Rent(COPY_ID, USER_ID, RENT_DATE, RETURN_DATE) VALUES(
		@copyid,
		@userid,
		GETDATE(),
		DATEADD(DAY, @days, GETDATE())
		)
END

--EXECUTE RENT_COPY 1, 2, 3;

--Procedure para devolução de filme alugado.
CREATE PROCEDURE RETURN_COPY @copyid INT
AS
	UPDATE Rent SET ACTUAL_RETURN_DATE = GETDATE() WHERE COPY_ID = @copyid AND ACTUAL_RETURN_DATE IS NULL;

--EXECUTE RETURN_COPY 2;

-- Procedure para criar uma cópia de filme.
CREATE PROCEDURE CREATE_COPY @movieid INT
AS
BEGIN
	INSERT INTO Copy(MOVIE_ID, CREATED_AT) VALUES(
		@movieid,
		GETDATE()
		)
END

-- EXECUTE CREATE_COPY 10;


--Função para ver se cópia está disponível.
CREATE FUNCTION AVAL_COPY(@movieid INT)
RETURNS TABLE
AS
RETURN(
	SELECT * FROM Copy COPY WHERE COPY.MOVIE_ID = @movieid AND COPY.ID NOT IN(
		SELECT C.ID
		FROM Copy C
		JOIN Rent R
			ON R.COPY_ID = C.ID
			AND C.MOVIE_ID = @movieid
			AND R.ACTUAL_RETURN_DATE IS NULL
		)
)

--SELECT * FROM AVAL_COPY(10)