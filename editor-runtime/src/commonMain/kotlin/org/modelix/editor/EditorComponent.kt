package org.modelix.editor

import kotlinx.html.TagConsumer
import kotlinx.html.div
import org.modelix.incremental.IncrementalIndex

open class EditorComponent(
    val engine: EditorEngine?,
    private val rootCellCreator: (EditorState) -> Cell
) : IProducesHtml {
    val state: EditorState = EditorState()
    private var selection: Selection? = null
    private val cellIndex: IncrementalIndex<CellReference, Cell> = IncrementalIndex()
    private var selectionUpdater: (() -> Selection?)? = null
    protected var codeCompletionMenu: CodeCompletionMenu? = null
    private var rootCell: Cell = rootCellCreator(state).also {
        it.editorComponent = this
        cellIndex.update(it.referencesIndexList)
    }

    fun selectAfterUpdate(newSelection: () -> Selection?) {
        selectionUpdater = newSelection
    }

    fun resolveCell(reference: CellReference): List<Cell> = cellIndex.lookup(reference)

    private fun updateRootCell() {
        val oldRootCell = rootCell
        val newRootCell = rootCellCreator(state)
        if (oldRootCell !== newRootCell) {
            oldRootCell.editorComponent = null
            newRootCell.editorComponent = this
            rootCell = newRootCell
            cellIndex.update(rootCell.referencesIndexList)
        }
    }

    open fun update() {
        updateRootCell()
        updateSelection()
    }

    fun getRootCell() = rootCell

    private fun updateSelection() {
        val updater = selectionUpdater
        selectionUpdater = null

        selection = updater?.invoke()
            ?: selection?.takeIf { it.isValid() }
            ?: selection?.update(this)
    }

    open fun changeSelection(newSelection: Selection) {
        selection = newSelection
        codeCompletionMenu = null
        update()
    }

    fun getSelection(): Selection? = selection

    fun showCodeCompletionMenu(
        anchor: LayoutableCell,
        position: CompletionPosition,
        entries: List<ICodeCompletionActionProvider>,
        pattern: String = "",
        caretPosition: Int? = null
    ) {
        codeCompletionMenu = CodeCompletionMenu(this, anchor, position, entries, pattern, caretPosition)
        codeCompletionMenu?.updateActions()
        update()
    }

    fun closeCodeCompletionMenu() {
        codeCompletionMenu = null
        update()
    }

    override fun isHtmlOutputValid(): Boolean = false

    override fun <T> produceHtml(consumer: TagConsumer<T>) {
        consumer.div("editor") {
            div(MAIN_LAYER_CLASS_NAME) {
                produceChild(rootCell.layout)
            }
            div("selection-layer relative-layer") {
                produceChild(selection)
            }
            div("popup-layer relative-layer") {
                produceChild(codeCompletionMenu)
            }
        }
    }

    fun dispose() {

    }

    open fun processKeyDown(event: JSKeyboardEvent): Boolean {
        try {
            if (event.knownKey == KnownKeys.F5) {
                clearLayoutCache()
                state.reset()
                return true
            }
            for (handler in listOfNotNull(codeCompletionMenu, selection)) {
                if (handler.processKeyDown(event)) return true
            }
            return false
        } finally {
            update()
        }
    }

    fun clearLayoutCache() {
        rootCell.descendantsAndSelf().forEach { it.clearCachedLayout() }
    }

    companion object {
        val MAIN_LAYER_CLASS_NAME = "main-layer"
    }
}

interface IKeyboardHandler {
    fun processKeyDown(event: JSKeyboardEvent): Boolean
}