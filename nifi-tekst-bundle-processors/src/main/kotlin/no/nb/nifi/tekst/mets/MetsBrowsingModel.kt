package no.nb.nifi.tekst.mets

/**
 * Simple, clean data classes for METS-browsing XML generation.
 * Direct mapping to XML structure without unnecessary abstraction.
 *
 * These are plain Kotlin data classes with no framework dependencies.
 * Serialization is handled by MetsBrowsingSerializer (METS v1) and Mets2BrowsingSerializer (METS v2).
 *
 * ## Version Compatibility Notes:
 *
 * These models support both METS v1 and METS v2 serialization. Some field names reflect METS v1
 * terminology but map appropriately when serializing to METS v2:
 *
 * - `admId` field → serializes to `ADMID` (v1) or `MDID` (v2)
 * - `xlinkHref` field → serializes to `xlink:href` (v1) or `LOCREF` (v2)
 * - `amdSec` list → serializes to multiple `amdSec` elements (v1) or single `mdSec` with `md` children (v2)
 *
 * The Agent model uses `type` and `otherType` fields. In METS v2, these are combined: if type="OTHER",
 * the otherType value is used as the TYPE attribute value.
 */

data class Mets(
    val objId: String,
    val metsHdr: MetsHdr,
    val amdSec: List<AmdSec>,
    val fileSec: FileSec,
    val structMap: StructMap
)

data class MetsHdr(
    val createDate: String,
    val lastModDate: String,
    val agent: Agent
)

/**
 * Agent information.
 *
 * METS v1: Serializes TYPE and OTHERTYPE as separate attributes
 * METS v2: Combines into single TYPE attribute (uses otherType value if type="OTHER")
 */
data class Agent(
    val role: String = "CREATOR",
    val type: String = "OTHER",
    val otherType: String = "Software",
    val name: String
)

data class AmdSec(
    val id: String,
    val techMD: TechMD
)

data class TechMD(
    val id: String,
    val mdWrap: MdWrap
)

data class MdWrap(
    val mdType: String = "NISOIMG",
    val xmlData: XmlData
)

data class XmlData(
    val mix: Mix
)

data class Mix(
    val basicImageInformation: BasicImageInformation? = null,
    val imageAssessmentMetadata: ImageAssessmentMetadata? = null
)

data class BasicImageInformation(
    val specialFormatCharacteristics: SpecialFormatCharacteristics? = null,
    val basicImageCharacteristics: BasicImageCharacteristics? = null
)

data class SpecialFormatCharacteristics(
    val jpeg2000: Jpeg2000
)

data class Jpeg2000(
    val encodingOptions: EncodingOptions
)

/**
 * JPEG2000 encoding options.
 *
 * MIX version compatibility:
 * - tiles: String format "1024x1024" for MIX v1.0, parsed to tileWidth/tileHeight for MIX v2.0
 * - qualityLayers, resolutionLevels: Same in both versions
 */
data class EncodingOptions(
    val tiles: String = "1024x1024",
    val qualityLayers: Int = 13,
    val resolutionLevels: Int = 6
) {
    /**
     * Parse tiles string into width and height for MIX v2.0
     * Example: "1024x1024" -> Pair(1024, 1024)
     */
    fun getTileDimensions(): Pair<Int, Int> {
        val parts = tiles.split("x")
        return if (parts.size == 2) {
            Pair(parts[0].toIntOrNull() ?: 1024, parts[1].toIntOrNull() ?: 1024)
        } else {
            Pair(1024, 1024)
        }
    }
}

data class BasicImageCharacteristics(
    val imageWidth: Int,
    val imageHeight: Int
)

data class ImageAssessmentMetadata(
    val spatialMetrics: SpatialMetrics
)

/**
 * Spatial metrics for image assessment.
 *
 * MIX version compatibility:
 * - samplingFrequencyUnit: Integer for MIX v1.0 (1=no unit, 2=inches, 3=cm)
 *                          String for MIX v2.0 ("no absolute unit of measurement", "in.", "cm")
 */
data class SpatialMetrics(
    val samplingFrequencyUnit: Int = 3,
    val xSamplingFrequency: SamplingFrequency,
    val ySamplingFrequency: SamplingFrequency
) {
    /**
     * Convert integer samplingFrequencyUnit (MIX v1.0) to string (MIX v2.0)
     */
    fun getSamplingFrequencyUnitString(): String {
        return when (samplingFrequencyUnit) {
            1 -> "no absolute unit of measurement"
            2 -> "in."
            3 -> "cm"
            else -> "cm"
        }
    }
}

data class SamplingFrequency(
    val numerator: Int,
    val denominator: Int
)

data class FileSec(
    val fileGrp: FileGrp
)

data class FileGrp(
    val id: String = "BROWSINGGRP",
    val file: List<FileEntry>
)

/**
 * File entry in the fileSec.
 *
 * Version differences:
 * - admId: Serialized as ADMID in METS v1, MDID in METS v2
 */
data class FileEntry(
    val id: String,
    val seq: Int? = null,
    val admId: String? = null,
    val use: String? = null,
    val mimeType: String? = null,
    val size: Long? = null,
    val checksum: String? = null,
    val checksumType: String? = null,
    val fLocat: List<FileLocation>? = null,
    val file: List<FileEntry>? = null  // Nested files for DO entries
)

/**
 * File location (FLocat element).
 *
 * Version differences:
 * - xlinkType, xlinkHref: Serialized as xlink:type and xlink:href in METS v1
 * - xlinkHref: Serialized as LOCREF attribute in METS v2 (xlinkType omitted)
 */
data class FileLocation(
    val locType: String,
    val xlinkType: String = "simple",
    val xlinkHref: String
)

data class StructMap(
    val type: String = "PHYSICAL",
    val div: Div
)

data class Div(
    val id: String? = null,
    val type: String,
    val order: Int? = null,
    val orderLabel: String? = null,
    val div: List<Div>? = null,  // Nested divs for PAGES container
    val fptr: FilePointer? = null,
    val fptrSingle: String? = null  // For simple fptr with just FILEID
)

data class FilePointer(
    val par: Par
)

data class Par(
    val area: List<Area>
)

data class Area(
    val fileId: String
)
