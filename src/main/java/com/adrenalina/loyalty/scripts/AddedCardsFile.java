package com.adrenalina.loyalty.scripts;

import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.AttributeEncryptor;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConf;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.datamodeling.encryption.providers.EncryptionMaterialsProvider;
import com.amazonaws.services.dynamodbv2.datamodeling.encryption.providers.SymmetricStaticProvider;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.loyalty.model.Member;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.crypto.spec.SecretKeySpec;

public class AddedCardsFile {

    private static class CardsFile {

        //Tue May 23 10:38:56 BST 2017 (linked date retrieved from dynamoDB)
        final private static DateTimeFormatter LINKED_DATE_FORMATTER_IN = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);

        // CSV FILE HEADER
        // ---------------
        final private static String HEADER = "Customer ID,Card Number,Added Date";

        private static String currentDir = System.getProperty("user.dir");

        //2017-05-27T11:52:57.405Z
        private static String now = Instant.now().toString();

        final private static DateTimeFormatter NOW_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        final private static DateTimeFormatter FILE_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        final private static LocalDateTime parsedDate = LocalDateTime.parse(now, NOW_FORMATTER);
        final private static String fileDate = parsedDate.format(FILE_DATE_FORMATTER);

        // CSV FILE OUTPUT
        // ---------------
        final private static String fileBase = "Customer card added " + fileDate;
        final private static String fileName = fileBase + ".csv";

        final public static String PATH = Paths.get(currentDir, fileName).toAbsolutePath().toString();
        final private static Path filePath = Paths.get(PATH);

        // SEARCH DATES
        // ------------
        final private static String SEARCHSTARTDATE = "2017-01-01T00:00:00";
        final private static String SEARCHENDDATE = "2027-12-24T00:00:00";

        // DATE FORMAT FOR THE CSV FILE OUTPUT
        // -----------------------------------
        final private static DateTimeFormatter LINKED_DATE_FORMATTER_OUT = DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ENGLISH);


        // OUTPUT FILE SETUP
        // -----------------
        private static BufferedWriter getWriter() {

            BufferedWriter writer = null;
            try {
                writer = Files.newBufferedWriter(filePath);
                System.out.printf("\nCreated file: %s\n\n", PATH);

            } catch (IOException ex) {
                System.out.printf("\nProblem with creating a writer file: [%s]\n\n", ex.getMessage());
                System.exit(1);
            }

            try {
                writer.write(HEADER + "\n");

            } catch (IOException ex) {
                 System.out.printf("\nProblem with writing header to the file: [%s]\n\n", ex.getMessage());
                System.exit(1);
            }

            return writer;
        }
    }

    // DYNAMODB SETUP
    // --------------
    private static class DAOConf {

        private static final String ENCRYPTION_ENCODING = "UTF-8";
        private static final String MESSAGE_DIGEST_ALGORITHM = "SHA-256";
        private static final String ENCRYPTION_ALGORITHM = "AES";
        private static final String SIGNING_ALGORITHM = "HmacSHA256";

        private String region = "us-west-1";

        private String nectarServiceTableNamePrefix = "loyalty-service-";

        // production
        private String encryptionKey = "xxxxxxxx";
        private String environmentTableNameSuffix = "-dev";

        public AmazonDynamoDB amazonDynamoDB() {

            AmazonDynamoDBClient client = new AmazonDynamoDBClient();
            if (StringUtils.isNoneEmpty(region)) {
                client.setRegion(RegionUtils.getRegion((region)));
            }

            return client;
        }

        public DynamoDB dynamoDB() {
            return new DynamoDB(amazonDynamoDB());
        }

        // Customize the table name
        private DynamoDBMapperConf getDynamoDBMapperConf() {
            DynamoDBMapperConf.Builder builder = DynamoDBMapperConf.builder();

            builder.setTableNameOverride(DynamoDBMapperConf.TableNameOverride.withTableNamePrefix(nectarServiceTableNamePrefix));
            builder.setTableNameResolver(new DynamoDBMapperConf.DefaultTableNameResolver() {
                @Override
                public String getTableName(Class<?> klass, DynamoDBMapperConf Conf) {

                    String tableName = super.getTableName(klass, Conf);

                    if (StringUtils.isNotBlank(environmentTableNameSuffix)) {
                        tableName += environmentTableNameSuffix;
                    }

                    return tableName.toLowerCase();
                }
            });

            builder.setPaginationLoadingStrategy(DynamoDBMapperConf.PaginationLoadingStrategy.ITERATION_ONLY);

            return builder.build();
        }

        public DynamoDBMapper dynamoDBMapperWithEncryptionSupport() throws NoSuchAlgorithmException, UnsupportedEncodingException {
            byte[] key = encryptionKey.getBytes(ENCRYPTION_ENCODING);

            MessageDigest sha = MessageDigest.getInstance(MESSAGE_DIGEST_ALGORITHM);
            key = sha.digest(key);
            byte[] key1 = Arrays.copyOf(key, 16); // use only first 128 bit for AES
            byte[] key2 = Arrays.copyOf(key, 8);  // use only first 64 bit for HmacSHA256

            SecretKeySpec cek = new SecretKeySpec(key1, ENCRYPTION_ALGORITHM);
            SecretKeySpec macKey = new SecretKeySpec(key2, SIGNING_ALGORITHM);
            EncryptionMaterialsProvider provider = new SymmetricStaticProvider(cek, macKey);

            return new DynamoDBMapper(amazonDynamoDB(), getDynamoDBMapperConf(), new AttributeEncryptor(provider));
        }
    }

    // ======================================================= main script =======================================================

    public static void main(String[] args) {

        DAOConf daoConf = new DAOConf();
        DynamoDBMapper mapper = null;

        try {
            mapper = daoConf.dynamoDBMapperWithEncryptionSupport();

        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            System.out.printf("Problem with DynamoDB encoding: %s", ex.getMessage());
            System.exit(1);
        }

        // Needed for retrieval of all batched/paged results
        Map<String, AttributeValue> lastKeyEvaluated = null;

        // ---> Construction of the DynamoDB query
        // ---------------------------------------
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#dt", "date");

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":namespace", new AttributeValue().withS("photobox"));
        expressionAttributeValues.put(":start_date", new AttributeValue().withS(CardsFile.SEARCHSTARTDATE));
        expressionAttributeValues.put(":end_date", new AttributeValue().withS(CardsFile.SEARCHENDDATE));

        DynamoDBScanExpression dynamoDBScanExpression = new DynamoDBScanExpression()
                .withFilterExpression("namespace = :namespace and #dt between :start_date and :end_date")
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withLimit(2000)
                .withExclusiveStartKey(lastKeyEvaluated);
        // ---------------------------------------

        BufferedWriter writer = CardsFile.getWriter();


        int i=0;
        do {
            // DynamoDB scan is used as opposed to dynamoDB query, because we are not searching on the primary key
            ScanResultPage<Member> scanPage = mapper.scanPage(Member.class, dynamoDBScanExpression);

            for (Member member : scanPage.getResults()) {
                i= i+1;

                LocalDateTime dynamoDbDate = LocalDateTime.parse(member.getDate().toString(), CardsFile.LINKED_DATE_FORMATTER_IN);
                String formattedDate = dynamoDbDate.format(CardsFile.LINKED_DATE_FORMATTER_OUT);

                StringBuilder recordBuilder = new StringBuilder(member.getMemberId().toString());
                recordBuilder.append(",");
                recordBuilder.append(member.getCard());
                recordBuilder.append(",");
                recordBuilder.append(formattedDate);
                recordBuilder.append("\n");

                String record = recordBuilder.toString();

                try {
                    writer.write(record);
                    //System.out.printf(record);

                } catch (IOException ex) {
                 System.out.printf("\nProblem with writing to the file: %s\n\n", ex.getMessage());
                }
            }

            dynamoDBScanExpression.setExclusiveStartKey(scanPage.getLastEvaluatedKey());

            try {
                writer.flush();
            } catch(IOException ex) {
                System.out.printf("Problem with flushing to the file: %s", ex.getMessage());
            }

        } while (dynamoDBScanExpression.getExclusiveStartKey() != null);

        try {
            writer.close();
        } catch(IOException ex) {
            System.out.printf("\nProblem with closing to the file: %s\n\n", ex.getMessage());
        }

        System.out.printf("\n>>> number of linked members = %d\n\n", i);
    }
}







