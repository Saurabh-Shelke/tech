import frappe
from frappe import _
from frappe.utils import now

def bom_custom(doc, method):
    """
    Syncs template BOM changes to all its variant BOMs on before_save.
    - Replaces BOM items with size-matching variant items if available.
    - Preserves qty and rate from existing variant BOM items.
    - Copies operations from template BOM to variant BOMs using SQL deletion and insertion.
    - Syncs routing field from template BOM.
    - Throws error and prevents submission if no matching-size variant exists for any item.
    """
    if not frappe.db.get_value("Item", doc.item, "has_variants"):
        return  # Not a template item

    variant_items = frappe.get_all("Item", filters={"variant_of": doc.item, "disabled": 0}, fields=["name"])
    if not variant_items:
        frappe.throw(
            "No variant items found for template item {}. Cannot submit template BOM without variants.".format(doc.item),
            title="No Variants Found"
        )

    errors = []
    missing_variant_errors = []

    for variant in variant_items:
        variant_code = variant.name
        parent_item = frappe.get_doc("Item", variant_code)

        # Get parent variant's Size attribute
        parent_size = next(
            (attr.attribute_value for attr in parent_item.get("attributes") if attr.attribute == "Size"),
            None
        )
        if not parent_size:
            frappe.msgprint("Variant item {} has no Size attribute. Skipping.".format(variant_code))
            continue

        # Get active BOMs for this variant
        variant_boms = frappe.get_all("BOM", filters={
            "item": variant_code,
            "is_active": 1,
            "docstatus": ["!=", 2]
        }, fields=["name", "docstatus"])

        for bom in variant_boms:
            try:
                variant_bom_name = bom.name
                variant_docstatus = bom.docstatus

                # Fetch existing items to preserve qty and rate
                qty_rate_map = {}
                existing_items = frappe.get_all("BOM Item",
                    filters={"parent": variant_bom_name},
                    fields=["item_code", "qty", "rate"]
                )
                for itm in existing_items:
                    qty_rate_map[itm.item_code] = {
                        "qty": itm.qty,
                        "rate": itm.rate
                    }

                # Clear existing items and operations from the variant BOM
                frappe.db.sql("DELETE FROM `tabBOM Item` WHERE parent = %s", variant_bom_name)
                frappe.db.sql("DELETE FROM `tabBOM Operation` WHERE parent = %s", variant_bom_name)

                # Copy items from template BOM to variant BOM
                for tpl_item in doc.items:
                    new_code = tpl_item.item_code

                    # Replace with variant if item has variants and size match is found
                    if tpl_item.has_variants:
                        matched_variant = frappe.db.get_value(
                            "Item Variant Attribute",
                            {
                                "variant_of": tpl_item.item_code,
                                "attribute": "Size",
                                "attribute_value": parent_size
                            },
                            "parent"
                        )
                        if matched_variant:
                            new_code = matched_variant
                        else:
                            error_msg = "No size-matching variant for {} in BOM {}. Cannot submit template BOM.".format(
                                tpl_item.item_code, variant_bom_name
                            )
                            missing_variant_errors.append(error_msg)
                            continue

                    item_data = {
                        "doctype": "BOM Item",
                        "parent": variant_bom_name,
                        "parenttype": "BOM",
                        "parentfield": "items",
                        "item_code": new_code,
                        "item_name": frappe.db.get_value("Item", new_code, "item_name") or tpl_item.item_name,
                        "do_not_explode": tpl_item.do_not_explode,
                        "bom_no": tpl_item.bom_no,
                        "allow_alternative_item": tpl_item.allow_alternative_item,
                        "is_stock_item": tpl_item.is_stock_item,
                        "qty": qty_rate_map.get(new_code, {}).get("qty", tpl_item.qty),
                        "uom": tpl_item.uom,
                        "stock_qty": tpl_item.stock_qty,
                        "stock_uom": tpl_item.stock_uom,
                        "conversion_factor": tpl_item.conversion_factor,
                        "rate": qty_rate_map.get(new_code, {}).get("rate", tpl_item.rate),
                        # "has_variants": tpl_item.has_variants,
                        "include_item_in_manufacturing": tpl_item.include_item_in_manufacturing,
                        "amount": tpl_item.amount,
                        "sourced_by_supplier": tpl_item.sourced_by_supplier,
                        "idx": tpl_item.idx
                    }
                    frappe.get_doc(item_data).insert(
                        ignore_permissions=True,
                        ignore_links=True,
                        ignore_if_duplicate=True,
                        ignore_mandatory=True
                    )

                # Copy operations from template BOM to variant BOM
                for op in doc.operations:
                    op_data = {
                        "doctype": "BOM Operation",
                        "parent": variant_bom_name,
                        "parenttype": "BOM",
                        "parentfield": "operations",
                        "operation": op.operation,
                        "description": op.description,
                        "workstation": op.workstation,
                        "time_in_mins": op.time_in_mins,
                        "fixed_time": op.fixed_time,
                        "sequence_id": op.sequence_id,
                        "idx": op.idx
                    }
                    frappe.get_doc(op_data).insert(
                        ignore_permissions=True,
                        ignore_links=True,
                        ignore_if_duplicate=True,
                        ignore_mandatory=True
                    )

                # Sync routing field from template BOM
                frappe.db.set_value("BOM", variant_bom_name, "routing", doc.routing)

                # Update modified timestamp for submitted BOMs
                if variant_docstatus == 1:
                    frappe.db.set_value("BOM", variant_bom_name, "modified", now())

                frappe.db.commit()

            except Exception as e:
                frappe.log_error("Error syncing variant BOM {}: {}".format(bom.name, str(e)), "BOM Sync")
                errors.append("Failed to update Variant BOM {}: {}".format(bom.name, str(e)))

    # Throw error for missing variants after processing all BOMs
    if missing_variant_errors:
        frappe.throw(
            "\n".join(missing_variant_errors),
            title="Missing Variant Items"
        )

    if errors:
        frappe.msgprint("\n".join(errors), title="Errors in Syncing Variant BOMs")
    else:
        frappe.msgprint("All variant BOMs updated successfully.")



