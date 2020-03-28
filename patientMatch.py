# Query mysql with python
from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config

# test code that prints one line of the database
def query_with_fetchone():
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM insert_name")

        row = cursor.fetchone()

        while row is not None:
            print(row)
            row = cursor.fetchone()

    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()

# Confidence test

def confidence():
    dbconfig = read_db_config()
    conn = MySQLConnection(**dbconfig)
    cursor = conn.cursor()

    OASALT = 'OATEST'

    firstQuery = 
    '''
        IF OBJECT_ID('tempdb..#tmpHASH') IS NOT NULL
            DROP TABLE #tmpHASH;
        WITH	cteImport
                AS (
                    SELECT DISTINCT TOP 10000
                        P.LAST_NAME LastName
                    , P.FIRST_NAME FirstName
                    , P.GENDER
                    , P.DATE_OF_BIRTH DateOfBirth
                    FROM
                        OfficeAlly_SS.dbo.PATIENTS P
                    ) 
    '''

    cursor.execute(firstQuery)

    all = cursor.fetchall()

    # Hashing
    '''
                cteHASH
                AS (
                    SELECT
                        X.idx
                    , X.Gender
                    , DateOfBirth
                    , (HASHBYTES('SHA1',
                                (@OASALT + (CONVERT([CHAR](8), [DateOfBirth], (112)) + '~' + [Gender]) + '~' + [FirstName])
                                + '~' + [LastName])) [FullNameHash]
                    , (HASHBYTES('SHA1',
                                (@OASALT + (CONVERT([CHAR](8), [DateOfBirth], (112)) + '~' + [Gender]) + '~'
                                    + LEFT([FirstName] + 'XXX', (3))) + '~' + LEFT([LastName] + 'XXX', (3)))) [ParHash]
                    , orgLastName
                    , orgFirstName
                    FROM
                        (
                        SELECT
                            ROW_NUMBER() OVER (ORDER BY C.LastName) idx
                        , CONVERT(DATETIME, DateOfBirth) [DateOfBirth]
                        , UPPER(ISNULL(CONVERT(CHAR(1), GENDER), 'U')) [Gender]
                        , UPPER(OADWH_HASH.dbo.RemoveNonAlphaNumeric(CONVERT(VARCHAR(25), FirstName))) [FirstName]
                        , UPPER(OADWH_HASH.dbo.RemoveNonAlphaNumeric(CONVERT(VARCHAR(25), LastName))) [LastName]
                        , C.LastName orgLastName
                        , C.FirstName orgFirstName
                --, '' MiddleName
                        FROM
                            cteImport C
                        ) X
                    ) ,
                cteHIDX
                AS (
                    SELECT
                        CH.idx
                    , ROW_NUMBER() OVER (ORDER BY CH.ParHash) hashID
                    , CH.ParHash
                    FROM
                        cteHASH CH
                    )
            SELECT
                C.idx
            , C.hashID
            , TH.ParHash
            , 'AP' + RIGHT('00000000' + CONVERT(VARCHAR(10), 100 + C.hashID), 8) xRefID
            , DATEADD(DAY, (CASE WHEN FLOOR((RAND() * 2) + 1) = 1 THEN -1
                                ELSE 1
                            END) * FLOOR(RAND() * (60) + 1), TH.DateOfBirth) NewDateOfBirth
            , TH.Gender
            , TH.orgLastName
            , TH.orgFirstName
            INTO
                #tmpHash
            FROM
                cteHASH TH
                JOIN cteHIDX C ON C.idx = TH.idx
            ORDER BY
                TH.idx 
'''















