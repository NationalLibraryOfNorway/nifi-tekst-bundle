package no.nb.nifi.tekst.mets

/**
 * Enum representing MIX (Metadata for Images in XML) schema versions.
 *
 * ## Version Differences:
 *
 * **MIX v1.0** (http://www.loc.gov/mix/v10):
 * - tiles: Simple string format like "1024x1024"
 * - samplingFrequencyUnit: Integer values (1=no unit, 2=inches, 3=cm)
 *
 * **MIX v2.0** (http://www.loc.gov/mix/v20):
 * - tiles: Container element with tileWidth and tileHeight as separate integers
 * - samplingFrequencyUnit: String values ("no absolute unit of measurement", "in.", "cm")
 * - Many elements changed from integral enumeration to text values
 */
enum class MixVersion {
    /**
     * MIX version 1.0
     * Namespace: http://www.loc.gov/mix/v10
     */
    MIX_1_0,

    /**
     * MIX version 2.0
     * Namespace: http://www.loc.gov/mix/v20
     */
    MIX_2_0
}
