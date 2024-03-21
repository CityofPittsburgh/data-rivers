import os


def append_to_text_field(incoming_table, field_name, delim):
    return f"""
    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
    SET alr.{field_name} = CONCAT(IFNULL(CONCAT(alr.{field_name}, '{delim}'), ''), tcc.{field_name})
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}` tcc
    WHERE alr.group_id = tcc.p_id AND
    (
        (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
        OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
        AND alr.child_ids != tcc.child_ids
    )
    """


def build_child_ticket_table(new_table, incoming_table, combined_children=True):
    if combined_children:
        alias = 'combined_children'
    else:
        alias = 'new_children'
    sql = f"""
    CREATE OR REPLACE TABLE `{os.environ['GCLOUD_PROJECT']}.qalert.{new_table}` AS
    (
        -- children never seen before and without a false parent
        WITH new_children AS
        (
        SELECT
            id, parent_ticket_id, anon_comments, pii_private_notes
        FROM
            `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}` new_c
        WHERE new_c.child_ticket = TRUE """
    if combined_children:
        sql += f"""
        AND new_c.id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)),
        -- children above plus the children of false parent tickets
        {alias} AS
        (
        SELECT *
        FROM new_children

        UNION ALL

        SELECT fp_id AS id, parent_ticket_id, anon_comments, pii_private_notes
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_prev_parent_now_child`
        """

    sql += f"""),
        -- from ALL children tickets, concatenate the necessary string data
        concat_fields AS
        (
        SELECT
            parent_ticket_id AS concat_p_id,
            STRING_AGG(id, ", ") AS child_ids,
            STRING_AGG(anon_comments, " <BREAK> ") AS anon_comments,
            STRING_AGG(pii_private_notes, " <BREAK> ") AS pii_private_notes
        FROM {alias}
        GROUP BY concat_p_id
        ),

        -- Sum all children within the linkage family
        child_count AS
        (
            SELECT
                parent_ticket_id AS p_id,
                COUNT(id) AS cts
            FROM {alias}
            GROUP BY p_id
        )

        -- Selection of all above processing into a temp table
        SELECT
            child_count.*,
            concat_fields.* EXCEPT (concat_p_id)
        FROM child_count
        JOIN concat_fields ON
        child_count.p_id = concat_fields.concat_p_id
    )
    """
    return sql


def delete_old_insert_new(cols, incoming_table):
    return f"""
    /*
    All tickets that ever receive an update (or are simply created) should be stored with their current status
    for historical purposes. This query does just that. The data are simply inserted into all_tickets_current_status. If
    the ticket has been seen before, it is already in all_tickets_current_status. Rather than simply add the newest ticket,
    this query also deletes the prior entry for that ticket. The key to understanding this is: The API does not return the
    FULL HISTORY of each ticket, but rather a snapshot of the ticket's current status. This means that if the status is
    updated
    multiple times between DAG runs, only the final status is recorded. While the FULL HISTORY has obvious value, this is
    not available and it less confusing to simply store a current snapshot of the ticket's history.

    all_tickets_current_status is currently (09/23) maintained for historical purposes.  This table has less value for
    analysis as the linkages between tickets are not taken into account.
    */

    DELETE FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`
    WHERE id IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}`);
    INSERT INTO `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`
    SELECT 
        {cols}
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.incoming_enriched`;
    """


def increment_ticket_counts(incoming_table):
    return f"""
    -- update existing entries inside all_linked_requests
    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
    SET alr.num_requests = tcc.cts + alr.num_requests
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}` tcc
    WHERE alr.group_id = tcc.p_id AND
    (
        (alr.child_ids NOT LIKE CONCAT('%, ', tcc.child_ids, '%')
        OR alr.child_ids NOT LIKE CONCAT('%', tcc.child_ids, ', %'))
        AND alr.child_ids != tcc.child_ids
    )
    """


def insert_new_parent(incoming_table, cols):
    return f"""
    /*
    This query check that a ticket has never been seen before (checks all_tickets_current_status) AND
    that the ticket is a parent. Satisfying both conditions means that the ticket needs to be placed in all_linked_requests
    There is one catch that is caused by the way tickets are manually linked: This newly recorded request is
    labeled as a parent. However, in the future the 311 operators may link this ticket with another
    existing parent and it will change into a child ticket. This means the original ticket was actually a "false_parent"
    ticket. Future steps in the DAG will handle that possibility, and for this query the only feasible option is to assume
    the ticket is correctly labeled.*/

    INSERT INTO `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
    (
    SELECT
        id as group_id,
        NULL AS child_tickets,
        1 as num_requests,
        IF(status_name = "closed", TRUE, FALSE) as parent_closed,
        {cols}

    FROM
        `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}`
    WHERE id NOT IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
    AND child_ticket = False
    )
    """


def replace_last_update(incoming_table, cols):
    sql = f"""
    /*
    Tickets are continually updated throughout their life. In the vast majority of cases a ticket will be created and
    then further processed (creating status changes) over a timeline which causes the ticket's lifespan to encompass
    multiple
    DAG runs. ONLY THE CURRENT STATUS of each ticket, which been updated since the last DAG run, is returned in the
    current DAG run. Thus, a ticket appears multiple times in our DAG.

    The only consequential information that consecutive updates contain are changes to the status, the time of the last
    update of the status, and the closure time (if applicable). The fact that child and parent tickets refer to the
    same underlying request creates the possibility that only a child OR parent could theoretically be updated. This
    occurred prior to 08/21 and it is currently (01/22) unclear if this will continue to happen (the API was updated to
    account for this). Most likely all relevant updates will by synchronized with the parent ticket. The most feasible
    solution to extracting update status/times is to take this information ONLY from the parent ticket and disregard
    changes to the child ticket. This query selects parent tickets which have been previously recorded in the system and
    simply extracts and updates the status timestamp data from those tickets. This data is then updated in
    all_linked_requests.
    */
    CREATE OR REPLACE TABLE  `{os.environ['GCLOUD_PROJECT']}.qalert.temp_update` AS
    (
    SELECT
        id,
        IF (status_name = "closed", TRUE, FALSE) AS p_closed, """
    sql += ", ".join(str(col) for col in cols)
    sql += f"""
    FROM  `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}`
    WHERE id IN (SELECT id FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_tickets_current_status`)
    AND child_ticket = FALSE
    );

    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
    SET alr.parent_closed = tu.p_closed
    FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_update` tu
    WHERE alr.group_id = tu.id;
    """

    for col in cols:
        sql += f"""
        UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` alr
        SET alr.{col} = tu.{col}
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.temp_update` tu
        WHERE alr.group_id = tu.id;
        """

    return sql


def update_linked_tix_info(incoming_table):
    return f"""
    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests` p
    SET p.child_tickets = ARRAY_CONCAT(p.child_tickets, c.child_tickets) 
    FROM (WITH new_child AS (
            SELECT id, parent_ticket_id, child_ticket, anon_comments, pii_private_notes
            FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}`
            WHERE child_ticket IS TRUE AND parent_ticket_id IN (
                    SELECT group_id  FROM `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
            )
         )
      SELECT ARRAY_AGG(STRUCT(id AS child_id, 
                              anon_comments AS child_comments, 
                              pii_private_notes AS child_notes
                              )
                      ) AS child_tickets, 
      parent_ticket_id
      FROM new_child
      GROUP BY parent_ticket_id
    ) c
    WHERE p.group_id = c.parent_ticket_id;
    
    UPDATE `{os.environ['GCLOUD_PROJECT']}.qalert.all_linked_requests`
    SET num_requests = ARRAY_LENGTH(child_tickets) + 1
    FROM (WITH new_child AS (
        SELECT DISTINCT parent_ticket_id
        FROM `{os.environ['GCLOUD_PROJECT']}.qalert.{incoming_table}`
        WHERE child_ticket IS TRUE
      ) 
      SELECT * FROM new_child
    ) 
    WHERE group_id = parent_ticket_id;
    """
