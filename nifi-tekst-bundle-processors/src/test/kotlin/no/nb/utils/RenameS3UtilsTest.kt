package no.nb.utils

import no.nb.models.RenameInstruction
import no.nb.utils.RenameUtils.extractIdFromFilename
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class RenameS3UtilsTest : S3TestBase() {
    val standardFilename= "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec"
    val testPrefix = "NEWSPAPER"

    //Helper functions to reduce duplicates in test
    private fun accessKey(name: String) = "$testPrefix/$standardFilename/representations/access/data/$name"
    private fun primaryKey(name: String) = "$testPrefix/$standardFilename/representations/primary/data/$name"

    private fun makeAccessObject(name: String, content: String = "") =
        putObject(accessKey(name), content)

    private fun makePrimaryObject(name: String, content: String = "") =
        putObject(primaryKey(name), content)

    @Nested
    inner class ExtractIdFromFilename {

        @Test
        fun `should extract id from standard filename`() {
            assertEquals(
                standardFilename,
                extractIdFromFilename("${standardFilename}_00001.tif")
            )
        }

        @Test
        fun `should extract id from filename with high page number`() {
            assertEquals(
                standardFilename,
                extractIdFromFilename("${standardFilename}_00099.tif")
            )
        }

        @Test
        fun `should extract id case insensitively`() {
            assertEquals(
                "TEKST_SOME-ID",
                extractIdFromFilename("TEKST_SOME-ID_00001.tif")
            )
        }

        @Test
        fun `should return null when filename has no page number`() {
            assertNull(extractIdFromFilename("no_page_number.tif"))
        }

        @Test
        fun `should return null when page number is not digits`() {
            assertNull(extractIdFromFilename("${standardFilename}_abc.tif"))
        }

        @Test
        fun `should return null when extension is not jp2`() {
            assertNull(extractIdFromFilename("${standardFilename}_00001.txt"))
        }

        @Test
        fun `should return null when filename has no extension or page number`() {
            assertNull(extractIdFromFilename(standardFilename))
        }

        @Test
        fun `should return null for empty string`() {
            assertNull(extractIdFromFilename(""))
        }
    }

    //RenameS3Files
    @Nested
    inner class RenameS3Files {

        @Test
        fun `should move keys to new locations`() {
            val originalName = "${standardFilename}_00003.tif"
            val newName = "${standardFilename}_00001.tif"

            makeAccessObject(originalName)
            makePrimaryObject(originalName)

            RenameS3Utils.renameS3Files(s3Client, BUCKET, listOf(RenameInstruction(originalName, newName)), testPrefix)
            assertTrue(keyExists(accessKey(newName)),
                "New access key should exist")
            assertTrue(keyExists(primaryKey(newName)),
                "New primary key should exist")
            assertFalse(keyExists(accessKey(originalName)),
                "Original access key should be deleted")
            assertFalse(keyExists(primaryKey(originalName)),
                "Original primary key should be deleted")
        }

        @Test
        fun `should correctly handle swap where two files exchange names`() {
            val nameA = "${standardFilename}_00001.tif"
            val nameB = "${standardFilename}_00002.tif"

            makeAccessObject(nameA, "content of A")
            makePrimaryObject(nameA, "content of A")
            makeAccessObject(nameB, "content of B")
            makePrimaryObject(nameB, "content of B")

            RenameS3Utils.renameS3Files(
                s3Client, BUCKET, listOf(
                    RenameInstruction(nameA, nameB),
                    RenameInstruction(nameB, nameA)
                ), testPrefix
            )

            assertTrue(keyExists(accessKey(nameA)), "Key A should exist after swap")
            assertTrue(keyExists(primaryKey(nameA)), "Key A primary should exist after swap")
            assertTrue(keyExists(accessKey(nameB)), "Key B should exist after swap")
            assertTrue(keyExists(primaryKey(nameB)), "Key B primary should exist after swap")
            assertTrue(listAllKeys().none { it.startsWith("tmp_") }, "No temp keys should remain after swap")
        }

        @Test
        fun `should skip instructions where original and new name are the same`() {
            val name = "${standardFilename}_00001.tif"
            val key = accessKey(name)
            makeAccessObject(name)

            RenameS3Utils.renameS3Files(s3Client, BUCKET, listOf(RenameInstruction(name, name)), testPrefix)

            assertTrue(keyExists(key), "Key should remain untouched when name is unchanged")
        }

        @Test
        fun `should process multiple instructions correctly`() {
            val instructions = (1..3).map { i ->
                RenameInstruction("${standardFilename}_0000${i}.tif", "${standardFilename}_0000${4 - i}.tif")
            }

            instructions.forEach { instruction ->
                makeAccessObject(instruction.originalName)
                makePrimaryObject(instruction.originalName)
            }

            RenameS3Utils.renameS3Files(s3Client, BUCKET, instructions, testPrefix)

            instructions.forEach { instruction ->
                assertTrue(keyExists(accessKey(instruction.newName)),
                    "New access key should exist: ${instruction.newName}")
                assertTrue(keyExists(primaryKey(instruction.newName)),
                    "New primary key should exist: ${instruction.newName}")
            }
        }

        @Test
        fun `should move an entire object to another itemId`() {
            val sourceItemId = "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec"
            val targetItemId = "tekst_019b4bb4-e1bf-8769-ab55-6ef015d62cfd"

            fun sourceAccess(name: String) = "$testPrefix/$sourceItemId/representations/access/data/$name"
            fun sourcePrimary(name: String) = "$testPrefix/$sourceItemId/representations/primary/data/$name"
            fun targetAccess(name: String) = "$testPrefix/$targetItemId/representations/access/data/$name"
            fun targetPrimary(name: String) = "$testPrefix/$targetItemId/representations/primary/data/$name"

            // Seed source object with 3 pages in both representations
            val pages = (1..3).map { "%05d".format(it) }
            pages.forEach { page ->
                putObject(sourceAccess("${sourceItemId}_$page.tif"), "access-$page")
                putObject(sourcePrimary("${sourceItemId}_$page.tif"), "primary-$page")
            }

            val instructions = pages.map { page ->
                RenameInstruction(
                    originalName = "${sourceItemId}_$page.tif",
                    newName = "${targetItemId}_$page.tif"
                )
            }

            RenameS3Utils.renameS3Files(s3Client, BUCKET, instructions, testPrefix)

            // Every file now lives under the target itemId
            pages.forEach { page ->
                assertTrue(keyExists(targetAccess("${targetItemId}_$page.tif")),
                    "Target access key should exist for page $page")
                assertTrue(keyExists(targetPrimary("${targetItemId}_$page.tif")),
                    "Target primary key should exist for page $page")
            }

            // Source object folder is fully cleared
            pages.forEach { page ->
                assertFalse(keyExists(sourceAccess("${sourceItemId}_$page.tif")),
                    "Source access key should be deleted for page $page")
                assertFalse(keyExists(sourcePrimary("${sourceItemId}_$page.tif")),
                    "Source primary key should be deleted for page $page")
            }
            assertTrue(listAllKeys().none { it.contains("/$sourceItemId/") },
                "No keys should remain under the source itemId folder")
            assertTrue(listAllKeys().none { it.startsWith("tmp_") },
                "No temp keys should remain after cross-object move")
        }

        @Test
        fun `should rename tif key in S3 only once when both tif and jp2 instructions target the same S3 key`() {
            // When both .tif and .jp2 exist on disk, addInstruction creates two rename instructions.
            // S3 only has a .tif key, so only one S3 rename should happen.
            val originalTif = "${standardFilename}_00003.tif"
            val originalJp2 = "${standardFilename}_00003.jp2"
            val newTif = "${standardFilename}_00001.tif"
            val newJp2 = "${standardFilename}_00001.jp2"

            makeAccessObject(originalTif)
            makePrimaryObject(originalTif)
            // No .jp2 keys in S3 — that is the invariant in production

            RenameS3Utils.renameS3Files(
                s3Client, BUCKET,
                listOf(
                    RenameInstruction(originalTif, newTif),
                    RenameInstruction(originalJp2, newJp2)
                ),
                testPrefix
            )

            assertTrue(keyExists(accessKey(newTif)), "New access .tif key should exist")
            assertTrue(keyExists(primaryKey(newTif)), "New primary .tif key should exist")
            assertFalse(keyExists(accessKey(originalTif)), "Original access .tif key should be deleted")
            assertFalse(keyExists(primaryKey(originalTif)), "Original primary .tif key should be deleted")
            assertTrue(listAllKeys().none { it.startsWith("tmp_") }, "No temp keys should remain")
        }

        @Test
        fun `should rename tif keys in S3 when rename instruction uses jp2 extension`() {
            // S3 always stores .tif; disk may have .jp2. Instructions are built from disk names,
            // so the instruction carries .jp2 even though S3 has .tif.
            val originalJp2 = "${standardFilename}_00003.jp2"
            val newJp2 = "${standardFilename}_00001.jp2"

            // Seed S3 with .tif keys (as it is in production)
            makeAccessObject("${standardFilename}_00003.tif")
            makePrimaryObject("${standardFilename}_00003.tif")

            RenameS3Utils.renameS3Files(s3Client, BUCKET, listOf(RenameInstruction(originalJp2, newJp2)), testPrefix)

            // S3 key at new position should exist as .tif
            assertTrue(keyExists(accessKey("${standardFilename}_00001.tif")),
                "New access key should exist as .tif after jp2-instructed rename")
            assertTrue(keyExists(primaryKey("${standardFilename}_00001.tif")),
                "New primary key should exist as .tif after jp2-instructed rename")
            // Original .tif keys should be gone
            assertFalse(keyExists(accessKey("${standardFilename}_00003.tif")),
                "Original access .tif key should be deleted")
            assertFalse(keyExists(primaryKey("${standardFilename}_00003.tif")),
                "Original primary .tif key should be deleted")
            assertTrue(listAllKeys().none { it.startsWith("tmp_") },
                "No temp keys should remain after rename")
        }

        @Test
        fun `should leave no temp keys after successful rename`() {
            makeAccessObject("${standardFilename}_00001.tif")
            makePrimaryObject("${standardFilename}_00001.tif")

            RenameS3Utils.renameS3Files(
                s3Client, BUCKET,
                listOf(RenameInstruction("${standardFilename}_00001.tif", "${standardFilename}_00002.tif")),
                testPrefix
            )

            assertTrue(listAllKeys().none { it.startsWith("tmp_") },
                "No temp keys should remain after successful rename")
        }

        @Test
        fun `should do nothing when rename instructions list is empty`() {
            val fileName = "${standardFilename}_00001.tif"
            val key = "${accessKey(standardFilename)}_00001.tif"
            makeAccessObject(fileName)

            RenameS3Utils.renameS3Files(s3Client, BUCKET, emptyList())

            assertTrue(keyExists(key), "Key should remain untouched when instructions are empty")
            assertEquals(1, listAllKeys().size, "Bucket should be unchanged")
        }

        @Test
        fun `should rename file that exists only in primary representation without access`() {
            val originalName = "${standardFilename}_00001.tif"
            val newName = "${standardFilename}_00002.tif"

            // Create file only in primary representation (not in access)
            makePrimaryObject(originalName)
            assertFalse(keyExists(accessKey(originalName)), "Original access key should not exist")
            assertTrue(keyExists(primaryKey(originalName)), "Original primary key should exist")

            // Perform rename
            RenameS3Utils.renameS3Files(s3Client, BUCKET, listOf(RenameInstruction(originalName, newName)), testPrefix)

            // Verify file exists at new name only in primary
            assertFalse(keyExists(accessKey(newName)), "New access key should not exist")
            assertTrue(keyExists(primaryKey(newName)), "New primary key should exist")
            assertFalse(keyExists(primaryKey(originalName)), "Original primary key should be deleted")
        }

        @Test
        fun `should rename file that exists only in access representation without primary`() {
            val originalName = "${standardFilename}_00001.tif"
            val newName = "${standardFilename}_00002.tif"

            // Create file only in access representation (not in primary)
            makeAccessObject(originalName)
            assertTrue(keyExists(accessKey(originalName)), "Original access key should exist")
            assertFalse(keyExists(primaryKey(originalName)), "Original primary key should not exist")

            // Perform rename
            RenameS3Utils.renameS3Files(s3Client, BUCKET, listOf(RenameInstruction(originalName, newName)), testPrefix)

            // Verify file exists at new name only in access
            assertTrue(keyExists(accessKey(newName)), "New access key should exist")
            assertFalse(keyExists(primaryKey(newName)), "New primary key should not exist")
            assertFalse(keyExists(accessKey(originalName)), "Original access key should be deleted")
        }
    }

    //Rollback
    @Nested
    inner class Rollback {

        @Test
        fun `should delete all temp keys when staging fails due to invalid source id`() {
            // An instruction with an invalid filename will cause extractIdFromFilename
            // to return null, triggering an exception mid-staging and rollback
            val validOriginal = "${standardFilename}_00001.tif"
            val validNew = "${standardFilename}_00002.tif"
            makeAccessObject(validOriginal)
            makePrimaryObject(validOriginal)

            val instructions = listOf(
                RenameInstruction(validOriginal, validNew),
                RenameInstruction("invalid-filename-no-page-number.tif", "other_00001.tif")
            )

            assertThrows(IllegalArgumentException::class.java) {
                RenameS3Utils.renameS3Files(s3Client, BUCKET, instructions, testPrefix)
            }

            // All temp keys should be cleaned up after rollback
            assertTrue(listAllKeys().none { it.startsWith("tmp_") },
                "No temp keys should remain after staging rollback")

            // Original keys should be untouched
            assertTrue(keyExists(accessKey(validOriginal)),
                "Original access key should be untouched after rollback")
            assertTrue(keyExists(primaryKey(validOriginal)),
                "Original primary key should be untouched after rollback")
        }

        @Test
        fun `should throw when source keys are missing`() {
            // No keys are put in the bucket — all instructions will fail
            assertThrows(IllegalStateException::class.java) {
                RenameS3Utils.renameS3Files(
                    s3Client, BUCKET,
                    listOf(RenameInstruction("${standardFilename}_00001.tif", "${standardFilename}_00002.tif")),
                    testPrefix
                )
            }

            // Bucket should remain empty — no temp keys created before the failure
            assertTrue(listAllKeys().isEmpty(), "Bucket should remain empty when source keys are missing")
        }
    }
}