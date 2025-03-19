package no.nb.nifi.tekst.metssections

import gov.loc.mets.FileType
import no.nb.commons.metadata.util.NamespacePrefixConstants
import no.nb.nifi.tekst.exception.MetsCreateException
import no.nb.productioncontroller.DirFilter
import no.nb.productioncontroller.FilterDefinition
import no.nb.productioncontroller.task.metscreate.util.ContentList
import no.nb.productioncontroller.task.metscreate.util.ContentPart
import no.nb.productioncontroller.util.xml.XmlHelper
import org.w3c.dom.Node
import java.io.File
import java.lang.reflect.Field
import java.lang.reflect.InvocationTargetException
import java.util.*

/**
 *
 * @author roger
 */
abstract class AbstractMetsSection {
    protected var metsContainerSectionConfig: Node? = null
    protected var contentLists: MutableList<ContentList> = ArrayList<ContentList>()
    protected var contentListIds: MutableList<String> = ArrayList()
    protected var namespaces: MutableList<NamespacePrefixConstants> = ArrayList<NamespacePrefixConstants>()
    protected var pageIndex: Int = -1
    protected var URNPrefix: String? = null
    protected var contentListSequence: Int = 0

    fun addContentList(contentList: ContentList) {
        if (!contentLists.contains(contentList)) {
            contentLists.add(contentList)
        }
    }

    protected fun getContentList(contentListID: String?): ContentList? {
        for (cList in contentLists) {
            if (cList.getContentListId().equals(contentListID)) {
                return cList
            }
        }
        return null
    }

    @Throws(MetsCreateException::class)
    protected fun getContentList(seqRef: Int): ContentList? {
        for (counter in contentListSequence..<contentLists.size) {
            val cList: ContentList = contentLists[counter]
            if (cList.getContentPartBySeq(seqRef) != null) {
                return cList
            }
        }
        return null
    }

    @Throws(MetsCreateException::class)
    protected fun getContentPart(seqRef: Int): ContentPart? {
        for (counter in contentListSequence..<contentLists.size) {
            val cList: ContentList = contentLists[counter]
            if (cList.getContentPartBySeq(seqRef) != null) {
                return cList.getContentPartBySeq(seqRef)
            }
        }
        return null
    }

    fun setPageIndex(pageIndex: Int) {
        this.pageIndex = pageIndex
    }

    fun getContentListIds(): List<String> {
        return contentListIds
    }

    protected fun setContentListSequence(seq: Int) {
        contentListSequence = seq
    }

    protected fun addNamespace(npc: NamespacePrefixConstants) {
        if (!namespaces.contains(npc)) {
            namespaces.add(npc)
        }
    }

    protected fun createFileType(id: String?): FileType {
        val fileType = FileType()
        fileType.id = id
        return fileType
    }

    /**
     *
     * @param metsSectionConfig The configuration for the METS section
     * @param urnPrefix The URN prefix to use.
     * @throws MetsCreateException Whenever initialization fails.
     */
    @Throws(MetsCreateException::class)
    fun init(metsSectionConfig: Node?, urnPrefix: String?) {
        this.metsContainerSectionConfig = metsSectionConfig
        URNPrefix = urnPrefix
        val contentListIdString: String = XmlHelper.getXmlParameterValue(metsSectionConfig, "contentList")
        if (contentListIdString != null) {
            contentListIds.addAll(Arrays.asList(*contentListIdString.split("[, ]+".toRegex()).dropLastWhile { it.isEmpty() }
                .toTypedArray()))
        }

        for (f in javaClass.declaredFields) {
            val an: MetsSectionParameter = f.getAnnotation<T>(MetsSectionParameter::class.java)
            if (an != null) {
                val configVal: String = XmlHelper.getXmlNodeValue(metsSectionConfig, f.name, null)
                if (configVal != null) {
                    setFieldValue(f, configVal)
                } else if (!an.optional()) {
                    throw MetsCreateException("Mandatory parameter '" + f.name + "' is missing for '" + javaClass.name + "'.")
                }
            }
        }
    }

    /**
     *
     * @param workingdir The working directory for this METS section
     * @return The generated METS section
     * @throws MetsCreateException Whenever METS section creation fails.
     */
    @Throws(MetsCreateException::class)
    abstract fun buildSection(workingdir: File?): Any?

    /**
     * Search for file with filepattern. Throws exception if no files found
     *
     * @param dir Directory containing files
     * @param regExp Regular expression for expected filename
     * @return A list of files that matches the supplied regexp.
     * @throws MetsCreateException Whenever retrieving the file fails.
     */
    @Throws(MetsCreateException::class)
    fun getFileByPattern(dir: File, regExp: String): File? {
        return getFileByPattern(dir, regExp, false)
    }


    fun getIdSequence(objectIndex: Int): String {
        val fileNum = if (contentLists[contentListSequence].isDualpage()) objectIndex / 2 + 1 else objectIndex + 1 //first page starts on 1
        var suffix = ""
        if (contentLists[contentListSequence].isDualpage()) {
            suffix = if (objectIndex % 2 == 0) "_F" else "_B"
        }
        return String.format("%04d%s", fileNum, suffix)
    }

    val namespaceLocations: List<Any>
        get() = namespaces

    @Throws(MetsCreateException::class)
    private fun setFieldValue(field: Field, value: String) {
        try {
            field.isAccessible = true
            if (field.type.isPrimitive) {
                when (field.type.name) {
                    "int" -> field[this] = value.toInt()
                    "long" -> field.setLong(this, value.toLong())
                    "double" -> field.setDouble(this, value.toLong().toDouble())
                    "boolean" -> field.setBoolean(this, value.toBoolean())
                    else -> throw NumberFormatException("Unhandled parameter type.")
                }
            } else if (field.type == String::class.java) {
                field[this] = value
            } else {
                val method = field.type.getDeclaredMethod("valueOf", String::class.java)

                field[this] = method.invoke(null, value)
            }
        } catch (ex: IllegalArgumentException) {
            throw MetsCreateException(
                ("Unable to set value for '" + javaClass.name + "."
                        + field.name + "', MSG: " + ex.localizedMessage), ex
            )
        } catch (ex: IllegalAccessException) {
            throw MetsCreateException(
                ("Unable to set value for '" + javaClass.name + "."
                        + field.name + "', MSG: " + ex.localizedMessage), ex
            )
        } catch (ex: NoSuchMethodException) {
            throw MetsCreateException(
                ("Unable to set value for '" + javaClass.name + "."
                        + field.name + "', MSG: " + ex.localizedMessage), ex
            )
        } catch (ex: SecurityException) {
            throw MetsCreateException(
                ("Unable to set value for '" + javaClass.name + "."
                        + field.name + "', MSG: " + ex.localizedMessage), ex
            )
        } catch (ex: InvocationTargetException) {
            throw MetsCreateException(
                ("Unable to set value for '" + javaClass.name + "."
                        + field.name + "', MSG: " + ex.localizedMessage), ex
            )
        }
    }

    enum class pageType {
        FRONT, BACK
    }

    companion object {
        const val URN_SERVICE_ROOT: String = "https://urn.nb.no/"
    }
}
