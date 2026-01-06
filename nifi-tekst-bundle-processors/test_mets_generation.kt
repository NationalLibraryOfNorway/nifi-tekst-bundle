import no.nb.nifi.tekst.mets.*

fun main() {
    val builder = MetsBrowsingBuilder("test")
        .withAgentName("test-agent")

    builder.addPage(
        pageNumber = 1,
        altoFile = AltoFileInfo(
            size = 1000,
            checksum = "abc123",
            urn = "URN:test",
            url = "file://test.xml"
        ),
        imageFile = ImageFileInfo(
            size = 2000,
            checksum = "def456",
            urn = "URN:test",
            url = "file://test.jp2"
        ),
        resolution = ImageResolution(width = 100, height = 200)
    )

    val mets = builder.build()
    val xml = MetsBrowsingSerializer.serialize(mets)

    println(xml)
}
