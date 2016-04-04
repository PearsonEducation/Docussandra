package com.pearson.docussandra.domain.objects;

import java.util.UUID;

/**
 * Supports the concept of a compound identifier. An Identifier is made up of
 * components, which are Object instances. The components are kept in order of
 * which they are added.
 *
 * For identifying Document types only.
 *
 * @author https://github.com/tfredrich
 * @since Aug 29, 2013
 */
public class DocumentIdentifier extends Identifier
{

    /**
     * Create an identifier with the given components. Duplicate instances are
     * not added--only one instance of a component will exist in the identifier.
     * Components should be passed in the order of significance: Database ->
     * Table -> Index -> Document
     *
     * @param components
     */
    public DocumentIdentifier(Object... components)
    {
        super(components);
    }

    /**
     * Constructor.
     *
     * Creates an Identifier from another Identifier.
     *
     * @param id
     */
    public DocumentIdentifier(Identifier id)
    {
        super(id.components().toArray());
    }

    /**
     * Gets the UUID for this document.
     *
     * @return
     */
    public UUID getUUID()
    {
        if (super.size() >= 3)
        {
            return (UUID) super.getComponent(2);
        }
        return null;
    }
}
