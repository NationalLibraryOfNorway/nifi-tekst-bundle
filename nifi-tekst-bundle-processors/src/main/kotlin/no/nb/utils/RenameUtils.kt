package no.nb.utils

object RenameUtils {

    /**
     * Extracts the item ID from a filename by stripping the trailing page number and extension.
     * Example: "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec_00003.jp2"
     *       -> "tekst_019a3aa3-d0af-7658-9a44-5df904c51bec"
     * Supports extensions: jp2, tif, tiff
     */
    public fun extractIdFromFilename(filename: String): String? {
        val regex = Regex("""^(.+)_\d+\.(jp2|tif|tiff)$""", RegexOption.IGNORE_CASE)
        return regex.find(filename)?.groups?.get(1)?.value
    }
}