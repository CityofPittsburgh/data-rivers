import MySQLdb
import csv
import os
import warnings

from datetime import datetime

from gcs_utils import now, storage_client, upload_file_gcs

warnings.filterwarnings('ignore', category=MySQLdb.Warning)
bucket = '{}_otrs'.format(os.environ['GCS_PREFIX'])

startTime = datetime.now()

OTRScon = MySQLdb.connect(host=os.envrion['OTRS_IP'], user=os.environ['OTRS_USER'], passwd=os.environ['OTRS_PW'],
                          db='otrs')

cursor_master = OTRScon.cursor()

cursor_master.execute("""SELECT q.name, 
t.id as 'ticket_id', 
t.tn as 'ticket_num', 
service.name as 'service',
ticket_type.name as 'type',
t.create_time as 'create.time', 
t.create_by as 'created.by', 
t.change_time as 'change.time', 
max(closed.create_time) as 'closed.time',
t.queue_id as 'final.queue', 
u.last_name as 'owner', 
r.last_name as 'responsible',
t.change_by as 'closed.by', 
ts.name as 'ticket.state', 
t.customer_id as 'cust.dept', 
t.customer_user_id as 'cust.name',
p.name as 'priority',
count(DISTINCT a.id) as comm_articles,
replace(t.title, char('/t'), '') as 'subject', 
(case when t.create_by = 1 then 1 else 0 end) as 'email', 
(case when t.ticket_state_id = 2 then 1 else 0 end) as 'success', 
(case when t.ticket_state_id = 5 then 1 else 0 end) as 'removed', 
(case when t.queue_id = 12 then 1 else 0 end) as 'closed.HD',
(case when t.ticket_state_id in(SELECT id FROM ticket_state WHERE type_id = 3) then 1 else 0 end) as 'closed', 
ty.name as 'last.action',
EXISTS(SELECT 1 FROM survey_request WHERE t.id = survey_request.ticket_id) AS 'survey.sent',
(CASE WHEN isnull(sr.vote_time) AND EXISTS (SELECT 1 FROM survey_request  WHERE t.id = survey_request.ticket_id) THEN  0 
     WHEN EXISTS(SELECT 1 FROM survey_request  WHERE t.id = survey_request.ticket_id) THEN  1
     ELSE 0 END) AS 'survey.response'
FROM ticket as t 
LEFT JOIN queue as q 
    ON t.queue_id = q.id
LEFT JOIN ticket_history as th
    ON  t.id = th.ticket_id AND t.change_time = th.create_time
LEFT JOIN ticket_history_type as ty
    ON th.history_type_id = ty.id
LEFT JOIN users as u 
    ON t.user_id = u.id
LEFT JOIN users as r 
   ON t.responsible_user_id = r.id
LEFT JOIN ticket_state as ts 
    ON t.ticket_state_id = ts.id 
JOIN ticket_type
    ON t.type_id = ticket_type.id
LEFT JOIN service
    ON t.service_id = service.id
LEFT JOIN ticket_history AS closed
    ON t.id = closed.ticket_id AND 
    closed.history_type_id = 27 AND 
        (closed.name LIKE'%%closed successful%%' OR
        closed.name LIKE '%%closed unsuccessful%%' OR
        closed.name LIKE'%%closed successfully%%' OR
        closed.name LIKE '%%closed unsuccessfully%%' OR
        closed.name LIKE '%%closed automatically%%' OR
        closed.name LIKE '%%closed with workaround%%')
LEFT JOIN ticket_priority as p
    ON t.ticket_priority_id = p.id
LEFT JOIN survey_request AS sr
    ON sr.ticket_id = t.id
LEFT JOIN article as a
    ON t.id = a.ticket_id AND a.article_sender_type_id <> 3

WHERE (t.create_time > '2013-01-01') 
    and (t.queue_id <> 24) 
    and (t.queue_id <> 3) 
    and (t.ticket_state_id <> 9) # Exclude merged tickets.

GROUP BY t.id;""")

with open('master.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_master.description])  # write headers
    csv_writer.writerows(cursor_master)

upload_file_gcs(bucket, 'master.csv', '{}/{}/{}_master.csv'.format(now.strftime('%Y'), now.strftime('%m').lower(),
                                                                   now.strftime('%Y-%m-%d')))
cursor_master.close()

cursor_moves = OTRScon.cursor()

cursor_moves.execute("""SELECT ticket_id, count(id) as 'moves'
FROM otrs.ticket_history
WHERE create_time >= '2013-01-01' AND history_type_id = 16
GROUP BY ticket_id;""")

with open('move_count.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_moves.description])
    csv_writer.writerows(cursor_moves)

upload_file_gcs(bucket, 'move_count.csv', '{}/{}/{}_move_count.csv'.format(now.strftime('%Y'),
                                                                           now.strftime('%m').lower(),
                                                                           now.strftime('%Y-%m-%d')))

cursor_moves.close()

cursor_owner = OTRScon.cursor()

cursor_owner.execute("""SELECT t.id as 'ticket_id',
min(th.create_time) as 'cur_owner.time'
FROM ticket as t
LEFT JOIN ticket_history as th
    ON t.id = th.ticket_id
    AND t.user_id = th.owner_id

WHERE t.create_time >= '2013-01-01'
    and (t.queue_id <> 24) 
    and (t.queue_id <> 3) 
    and (t.ticket_state_id <> 9) # Exclude merged tickets.

GROUP BY t.id;""")

with open('owner_time.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_owner.description])
    csv_writer.writerows(cursor_owner)

upload_file_gcs(bucket, 'owner_time.csv', '{}/{}/{}_owner_time.csv'.format(now.strftime('%Y'),
                                                                            now.strftime('%m').lower(),
                                                                            now.strftime('%Y-%m-%d')))
cursor_owner.close()

cursor_survey = OTRScon.cursor()

cursor_survey.execute("""SELECT sr.ticket_id,
sr.send_time,
sr.vote_time,
(CASE 
    WHEN q1.vote_value = 1 THEN 5
    WHEN q1.vote_value = 2 THEN 4
    WHEN q1.vote_value = 3 THEN 3
    WHEN q1.vote_value = 4 THEN 2
    WHEN q1.vote_value = 5 THEN 1
    ELSE ""
end) AS 'Q1',
(CASE 
    WHEN q2.vote_value = 6 THEN 5
    WHEN q2.vote_value = 7 THEN 4
    WHEN q2.vote_value = 8 THEN 3
    WHEN q2.vote_value = 9 THEN 2
    WHEN q2.vote_value = 10 THEN 1
    ELSE ""
end) AS 'Q2',
CONCAT('"',replace(q3.vote_value, '$html/text$', ''),'"') AS 'Com1',
(CASE 
    WHEN q4.vote_value = 11 THEN 5
    WHEN q4.vote_value = 12 THEN 4
    WHEN q4.vote_value = 13 THEN 3
    WHEN q4.vote_value = 14 THEN 2
    WHEN q4.vote_value = 15 THEN 1
    ELSE ""
end) AS 'Q3',
CONCAT('"',replace(q5.vote_value, '$html/text$', ''),'"') AS 'Com2'
FROM survey_request AS sr 
JOIN survey_vote AS q1 ON q1.request_id = sr.id AND q1.question_id = 1
JOIN survey_vote AS q2 ON q2.request_id = sr.id AND q2.question_id = 2
JOIN survey_vote AS q3 ON q3.request_id = sr.id AND q3.question_id = 3
JOIN survey_vote AS q4 ON q4.request_id = sr.id AND q4.question_id = 4
JOIN survey_vote AS q5 ON q5.request_id = sr.id AND q5.question_id = 5
WHERE (sr.ticket_id <> 46712) AND (sr.ticket_id <> 46728) AND sr.survey_id = 1
GROUP BY sr.id;""")

with open('survey_responses.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_survey.description])
    csv_writer.writerows(cursor_survey)

upload_file_gcs(bucket, 'survey_responses.csv', '{}/{}/{}_survey_responses.csv'.format(now.strftime('%Y'),
                                                                                       now.strftime('%m').lower(),
                                                                                       now.strftime('%Y-%m-%d')))
cursor_survey.close()

cursor_survey2 = OTRScon.cursor()

cursor_survey2.execute("""SELECT sr.ticket_id,
sr.send_time,
sr.vote_time,
(CASE
    WHEN q1.vote_value = 16 THEN 1
    WHEN q1.vote_value = 17 THEN 2
    WHEN q1.vote_value = 18 THEN 3
    WHEN q1.vote_value = 19 THEN 4
    WHEN q1.vote_value = 20 THEN 5
END) as 'Communication',
(CASE
    WHEN q2.vote_value = 21 THEN 1
    WHEN q2.vote_value = 22 THEN 2
    WHEN q2.vote_value = 23 THEN 3
    WHEN q2.vote_value = 24 THEN 4
    WHEN q2.vote_value = 25 THEN 5
END) as 'Resolution',
CONCAT('"',replace(q3.vote_value, '$html/text$', ''),'"') AS 'Comment'
FROM otrs.survey_request as sr
JOIN survey_vote AS q1 ON q1.request_id = sr.id AND q1.question_id = 6
JOIN survey_vote AS q2 ON q2.request_id = sr.id AND q2.question_id = 7
JOIN survey_vote AS q3 ON q3.request_id = sr.id AND q3.question_id = 8

WHERE sr.survey_id = 2
GROUP BY sr.id;""")

with open('survey_responses_2.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_survey2.description])
    csv_writer.writerows(cursor_survey2)

upload_file_gcs(bucket, 'survey_responses_2.csv', '{}/{}/{}_survey_responses_2.csv'.format(now.strftime('%Y'),
                                                                                         now.strftime('%m').lower(),
                                                                                         now.strftime('%Y-%m-%d')))
cursor_survey2.close()

cursor_history = OTRScon.cursor()

cursor_history.execute("""SELECT th.ticket_id,
    q.name, th.create_time as time,
    ty.name as 'action',
    st.name as 'state'

    FROM otrs.ticket_history as th

    JOIN otrs.ticket as t
    ON th.ticket_id = t.id
    JOIN otrs.ticket_history_type as ty
    ON th.history_type_id = ty.id
    JOIN otrs.queue as q
    ON th.queue_id = q.id
    JOIN otrs.ticket_state as st
    ON th.state_id = st.id

    WHERE th.history_type_id IN (1,16,27) AND
    t.create_time >= '2013-01-01' AND
    th.queue_id IN (6,7,12,15,16,42,43,45,46) #Default, HelpDesk, Needs Assigned, Tier_1, Andon, Director Review, Ordering, Staging

    ORDER BY th.ticket_id, th.create_time ASC;""")

with open('ticket_history.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_history.description])  # write headers
    csv_writer.writerows(cursor_history)

upload_file_gcs(bucket, 'ticket_history.csv', '{}/{}/{}_ticket_history.csv'.format(now.strftime('%Y'),
                                                                                   now.strftime('%m').lower(),
                                                                                   now.strftime('%Y-%m-%d')))
cursor_history.close()

cursor_comms = OTRScon.cursor()

cursor_comms.execute("""SELECT t.id AS 'ticket_id',
    min(com.create_time) AS 'first_com',
    max(com.create_time) AS 'recent_com',
    (CASE WHEN max(com.create_time) = min(com.create_time) THEN 'True' ELSE 'False' END) AS 'same_com_time',
    max(th.create_time) AS 'last_move.time'

    FROM ticket AS t

    LEFT JOIN article AS com
    ON t.id = com.ticket_id AND com.communication_channel_id <> 3 AND com.article_sender_type_id = 1
    LEFT JOIN ticket_history AS th
    ON t.id = th.ticket_id AND t.queue_id = th.queue_id AND th.history_type_id IN (1,16)

    WHERE t.create_time >= '2013-01-01'
    and (t.queue_id <> 24)
    and (t.queue_id <> 3)
    and (t.ticket_state_id <> 9) # Exclude merged tickets.

    GROUP BY ticket_id

    ORDER BY ticket_id ASC;""")

with open('comm_times.csv', 'w') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cursor_comms.description])  # write headers
    csv_writer.writerows(cursor_comms)

upload_file_gcs(bucket, 'comm_times.csv', '{}/{}/{}_comm_times.csv'.format(now.strftime('%Y'), now.strftime('%m').lower(),
                                                                       now.strftime('%Y-%m-%d')))

cursor_comms.close()
