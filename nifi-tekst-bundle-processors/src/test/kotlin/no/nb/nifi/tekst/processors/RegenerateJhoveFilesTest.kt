package no.nb.nifi.tekst.processors

import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Utility test to regenerate JHOVE XML files for test resources.
 */
class RegenerateJhoveFilesTest {

    private val projectFolder = Paths.get("").toAbsolutePath().toString()
    private val testResourcesBase = Paths.get(
        projectFolder,
        "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72"
    )
    private val descriptiveXml = Paths.get(
        projectFolder,
        "src/test/resources/tekst_ee11f8dd-512a-49c2-95f0-03ece023fe72-METS_BROWSING.xml"
    )

    @Test
    @Disabled("Run manually to regenerate JHOVE files")
    fun regenerateAllJhoveFiles() {
        val objectFolder = TestObjectFolderHelper.createTempObjectFolder(testResourcesBase, descriptiveXml)

        try {
            runJhove(objectFolder)
            println("JHOVE files regenerated in: $objectFolder")
        } finally {
            TestObjectFolderHelper.deleteTempObjectFolder(objectFolder)
        }
    }

    private fun runJhove(objectFolder: Path) {
        val runner = TestRunners.newTestRunner(Jhove::class.java)
        runner.setProperty(Jhove.OBJECT_FOLDER, objectFolder.toString())
        runner.setProperty(Jhove.BEHAVIOUR_ON_ERROR, "fail")

        runner.enqueue("test")
        runner.run()

        runner.assertTransferCount(Jhove.SUCCESS_RELATIONSHIP, 1)
    }
}
