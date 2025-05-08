
# import frappe


# @frappe.whitelist()
# def bom_custom(*args, **kwargs):

import frappe
from frappe import _

def bom_custom(doc, method):
    """
    Syncs changes from a template BOM to its variant BOMs on before_save.
    Preserves qty and rate from existing variant BOM items.
    Updates both draft and submitted variant BOMs.
    """
    # Log for debugging
    frappe.log_error(f"bom_custom called for BOM: {doc.name}, method: {method}", "BOM Custom Sync")
    
    # Check if the BOM’s item is a template (has variants)
    if not frappe.db.get_value("Item", doc.item, "has_variants"):
        frappe.msgprint(_("This BOM is not a template with variants."))
        return
    
    # Fetch all variant items of this template item
    variant_items = frappe.get_all("Item",
        filters={"variant_of": doc.item},
        fields=["name"]
    )
    
    if not variant_items:
        frappe.msgprint(_("No variant items found for this template."))
        return
    
    # Get names of all BOMs for variant items (excluding inactive and the template BOM)
    variant_item_names = [item.name for item in variant_items]
    variant_boms = frappe.get_all("BOM",
        filters={
            "item": ["in", variant_item_names],
            "is_active": 1,
            "name": ["!=", doc.name]
        },
        fields=["name", "docstatus"]
    )
    
    if not variant_boms:
        frappe.msgprint(_("No active variant BOMs found for this template’s variants."))
        return
    
    errors = []
    for var in variant_boms:
        variant_bom_name = var.name
        variant_docstatus = var.docstatus
        
        try:
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
            for tmpl_item in doc.items:
                code = tmpl_item.item_code
                item_data = {
                    "doctype": "BOM Item",
                    "parent": variant_bom_name,
                    "parenttype": "BOM",
                    "parentfield": "items",
                    "item_code": code,
                    "item_name": tmpl_item.item_name,
                    "do_not_explode": tmpl_item.do_not_explode,
                    "bom_no": tmpl_item.bom_no,
                    "allow_alternative_item": tmpl_item.allow_alternative_item,
                    "qty": qty_rate_map.get(code, {}).get("qty", tmpl_item.qty),
                    "uom": tmpl_item.uom,
                    "stock_qty": tmpl_item.stock_qty,
                    "stock_uom": tmpl_item.stock_uom,
                    "conversion_factor": tmpl_item.conversion_factor,
                    "rate": qty_rate_map.get(code, {}).get("rate", tmpl_item.rate),
                    "has_variants": tmpl_item.has_variants,
                    "include_item_in_manufacturing": tmpl_item.include_item_in_manufacturing,
                    "amount": tmpl_item.amount,
                    "sourced_by_supplier": tmpl_item.sourced_by_supplier,
                    "idx": tmpl_item.idx
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
            
            # Update modified timestamp for submitted BOMs
            if variant_docstatus == 1:
                frappe.db.set_value("BOM", variant_bom_name, "modified", frappe.utils.now())
            
            frappe.db.commit()
            
        except Exception as e:
            frappe.log_error(f"Error updating Variant BOM {variant_bom_name}: {str(e)}", "Sync Variant BOMs")
            errors.append(f"Failed to update Variant BOM {variant_bom_name}: {str(e)}")
    
    # Display all errors after processing
    if errors:
        frappe.msgprint("\n".join(errors), title=_("Errors in Syncing Variant BOMs"))
    else:
        frappe.msgprint(_("Successfully updated all variant BOMs."))