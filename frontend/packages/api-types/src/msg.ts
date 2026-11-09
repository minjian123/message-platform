/**
 * 产品契约生成类型：msg
 *
 * 本文件由 `frontend/packages/api-types/scripts/generate.mjs` 生成，请勿手工修改。
 * 来源：deploy/contracts/msg.json（产品契约快照，唯一事实源）。
 * 重新生成：pnpm run api-types:gen
 */
export interface paths {
    "/": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Root
         * @description 应用信息。
         *
         *     Returns:
         *         ApiResponse: {code, message, data:{name, version}}。
         */
        get: operations["root__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/ack": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Ack
         * @description 确认一条消息（位点提交值 = `offset + 1`，提交失败不前移）。
         *
         *     需要 msg:consume 权限；须是该分区的当前持有者（组不存在 350013，成员失效或分区已易主 350014）。
         *     位点越界 350002，消息不在投递中 350003，消息已因重投超限转入死信 350007。
         */
        post: operations["ack_api_v1_msg_ack_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/acls": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Acls
         * @description ACL 规则列表（分页 + 命名空间 / 主题 / 主体 / 动作 / 状态过滤）。
         *
         *     需要 msg:manage 权限；按租户隔离；命名空间过滤值非法 350010，过滤枚举非法 10001。
         */
        get: operations["list_acls_api_v1_msg_admin_acls_get"];
        /**
         * Upsert Acls
         * @description 批量授予 / 更新 ACL（按唯一键 upsert；整批校验、整批成功 / 失败）。
         *
         *     需要 msg:manage 权限 + 目标主题 manage ACL；主题不存在 350001，命名空间非法 350010，
         *     ACL 拒绝 350027，参数非法 10001。新建 / 恢复记 grant，改 effect 记 update，幂等不变更不写审计。
         */
        put: operations["upsert_acls_api_v1_msg_admin_acls_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/acls/{acl_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        post?: never;
        /**
         * Revoke Acl
         * @description 收回一条 ACL（置 `status=disabled`；幂等）。
         *
         *     需要 msg:manage 权限 + 目标主题 manage ACL；规则不存在 / 不属于当前租户返回 `changed=false`，
         *     ACL 拒绝 350027。
         */
        delete: operations["revoke_acl_api_v1_msg_admin_acls__acl_id__delete"];
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/delayed": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Delayed
         * @description 延迟中消息列表（到期时刻升序，最急的在前；只读，不触发转投）。
         *
         *     需要 msg:query 权限；按租户隔离。命名空间非法 350010，主题不存在或未启用 350001，
         *     分区号越界 350009，档位非法 10001；`size` 上界 100（越界 10001）。
         */
        get: operations["list_delayed_api_v1_msg_admin_delayed_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/delayed/summary": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Delay Summary
         * @description 延迟档位分布与堆积提示（服务端当前时间计算剩余时间；只读，不触发转投）。
         *
         *     命名空间非法 350010，主题不存在 350001，分区越界 350009。
         */
        get: operations["get_delay_summary_api_v1_msg_admin_delayed_summary_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/dlq": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Dead Letters
         * @description 死信列表（默认排除已归档）。
         *
         *     需要 msg:query 权限；按租户隔离；无主题 `describe` 授权的行不返回。
         */
        get: operations["list_dead_letters_api_v1_msg_admin_dlq_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/dlq/{dlq_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Dead Letter
         * @description 死信详情（含失败上下文）。
         *
         *     需要 msg:query 权限；不存在或跨租户返回 350029；无主题 describe 授权返回 350027。
         */
        get: operations["get_dead_letter_api_v1_msg_admin_dlq__dlq_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/dlq/{dlq_id}/archive": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Archive Dead Letter
         * @description 归档死信（从活跃列表隐藏，原始消息保留）。
         *
         *     需要 msg:manage 权限；`replaying` / `archived` 返回 350030。
         */
        post: operations["archive_dead_letter_api_v1_msg_admin_dlq__dlq_id__archive_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/dlq/{dlq_id}/ignore": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Ignore Dead Letter
         * @description 忽略死信（原因必填）。
         *
         *     需要 msg:manage 权限；状态非 pending 350030；原因空白 / 超长 10001。
         */
        post: operations["ignore_dead_letter_api_v1_msg_admin_dlq__dlq_id__ignore_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/dlq/{dlq_id}/replay": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Replay Dead Letter
         * @description 重放死信（按原分区 / offset 建单条重放任务）。
         *
         *     需要 msg:manage 权限；状态非 pending 350030；原记录不存在 350031。
         */
        post: operations["replay_dead_letter_api_v1_msg_admin_dlq__dlq_id__replay_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/groups": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Groups
         * @description 消费组列表（组名升序；可按命名空间 / 主题过滤）。
         *
         *     需要 msg:query 权限；按租户隔离；主题已删的孤儿组记录不展示。命名空间过滤值非法返回 350010。
         */
        get: operations["list_groups_api_v1_msg_admin_groups_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/groups/{namespace}/{topic}/{group}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Group
         * @description 消费组详情（状态 / 世代 / 成员 / 分配 / 各分区位点与积压）。
         *
         *     需要 msg:query 权限；主题不存在 350001，组不存在 350013。
         */
        get: operations["get_group_api_v1_msg_admin_groups__namespace___topic___group__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/messages": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Search Messages
         * @description 按组合条件检索消息头（只返回头与摘要，不返回正文）。
         *
         *     命名空间非法 350010，主题不存在 350001，分区越界 350009；
         *     条件区间倒置 / 分页越界 10001。
         */
        get: operations["search_messages_api_v1_msg_admin_messages_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/offsets/reset": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Reset Offset
         * @description 重置某组在某主题某分区上的已提交位点（可倒退可前跳，需 `confirm=true`）。
         *
         *     需要 msg:manage 权限；`confirm` 缺失 10001；主题不存在 350001、状态不允许 350028；
         *     组不存在 350013；分区号越界 350009；位点早于分区起点 350003（消息已被保留期清理）；
         *     位点超过 LEO 350002；写审计由 `OffsetResetter` 以 `record_ref=msg.offset` 落库。
         */
        post: operations["reset_offset_api_v1_msg_admin_offsets_reset_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/overview": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Overview
         * @description 四张数字卡（入口吞吐 / 当前积压 / 死信数 / 磁盘使用率）。
         *
         *     快照尚未产出时四值返回 0 且 `snapshot_available=false`，不回源扫描。
         */
        get: operations["get_overview_api_v1_msg_admin_overview_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/partitions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Partitions
         * @description 分区详情（按分区号升序；只读）。
         *
         *     命名空间非法 350010，主题不存在 350001，分区越界 350009，组不存在 350013。
         */
        get: operations["list_partitions_api_v1_msg_admin_partitions_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/quotas": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Quotas
         * @description 查询六维配额（含缺省值 / 当前用量 / 状态）。
         *
         *     Args:
         *         tenant: 当前租户。
         *         quota: 配额服务。
         *         namespace: 作用域。
         *
         *     Returns:
         *         ApiResponse[QuotaView]: 六维视图。
         */
        get: operations["get_quotas_api_v1_msg_admin_quotas_get"];
        /**
         * Update Quotas
         * @description 批量调整配额（整批校验、整批失败；提交后即时生效并写审计）。
         *
         *     Args:
         *         request: 批量调整请求。
         *         tenant: 当前租户。
         *         quota: 配额服务。
         *         uow: 平台库工作单元。
         *         sink: 审计出口。
         *
         *     Returns:
         *         ApiResponse[QuotaView]: 调整后的六维视图。
         *
         *     Raises:
         *         ParamError: 未知维度 / 重复作用域 / 软限非法（10001）。
         */
        put: operations["update_quotas_api_v1_msg_admin_quotas_put"];
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/replay": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Replay
         * @description 重放任务列表（创建时间倒序；可按状态 / 命名空间 / 主题 / 创建时间过滤）。
         *
         *     需要 msg:query 权限；按租户隔离；无主题 `describe` 权限的任务不出现在 items（`total` 仍为过滤前
         *     计数，与 06-1 组列表同口径）。状态取值非法 / 分页越界 / 创建时间区间颠倒返回 10001。
         */
        get: operations["list_replay_api_v1_msg_admin_replay_get"];
        put?: never;
        /**
         * Create Replay
         * @description 建重放任务（按条件把历史再写一遍，返回任务快照）。
         *
         *     需要 msg:manage 权限；范围条件左闭右开、跨类相与，`partitions` 缺省为全部分区。
         *     命名空间非法 350010，主题不存在或未启用 350001，分区号越界 350009，区间次序非法 10001。
         *     任务落库前采集影响基线（受影响组 + 分区级积压）；入口先写「建任务」审计，再惰性启动执行器
         *     （保证留痕顺序先创建后终态）。
         */
        post: operations["create_replay_api_v1_msg_admin_replay_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/replay/{task_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Replay
         * @description 查重放任务进度（已投递 / 总量 + 状态 + 首末记录标识 + 受影响组影响视图）。
         *
         *     需要 msg:query 权限；任务不存在或不属于当前租户返回 350016。任务已终结且尚无影响快照时，
         *     本次查询顺带落一次快照（之后恒返回该快照），任务本身的状态与进度不变。
         */
        get: operations["get_replay_api_v1_msg_admin_replay__task_id__get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/replay/{task_id}/cancel": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Cancel Replay
         * @description 取消任务（非终态可取消；已写入的记录不回滚——重放是新增写入，消费端仍按 message_id 幂等）。
         *
         *     需要 msg:manage 权限；任务不存在 350016，已是终态 350017。取消在请求侧写终态审计（操作人可查）。
         */
        post: operations["cancel_replay_api_v1_msg_admin_replay__task_id__cancel_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/replay/{task_id}/pause": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Pause Replay
         * @description 暂停任务（批边界生效：当前批写完才停，进度与写入始终一致）。
         *
         *     需要 msg:manage 权限；任务不存在 350016，已暂停或已终态 350017。
         */
        post: operations["pause_replay_api_v1_msg_admin_replay__task_id__pause_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/replay/{task_id}/resume": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Resume Replay
         * @description 恢复任务（仅接受已暂停的任务）。
         *
         *     需要 msg:manage 权限；任务不存在 350016，未处于暂停 350017。
         */
        post: operations["resume_replay_api_v1_msg_admin_replay__task_id__resume_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/topics": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * List Topics
         * @description 主题列表（页码分页 + 命名空间 / 名称过滤）。
         *
         *     需要 msg:query 权限；按租户隔离；命名空间过滤值非法返回 350010。
         */
        get: operations["list_topics_api_v1_msg_admin_topics_get"];
        put?: never;
        /**
         * Create Topic
         * @description 创建主题（指定分区数，返回完整元数据）。
         *
         *     需要 msg:manage 权限；`tenant_id` 取自基座解析链（请求体不接收）；同租户 / 命名空间下重名
         *     返回 350008，分区数或保留期越界返回 350009，命名空间非已启用 module_key 返回 350010，
         *     主题数配额已满返回 350025。
         */
        post: operations["create_topic_api_v1_msg_admin_topics_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/topics/{topic_id}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Get Topic
         * @description 主题详情（含分区列表；`deleting` 时含清理进度）。
         *
         *     需要 msg:query 权限；不存在或不属于当前租户返回 350001。
         */
        get: operations["get_topic_api_v1_msg_admin_topics__topic_id__get"];
        put?: never;
        post?: never;
        /**
         * Delete Topic
         * @description 删主题（**受理即返回**：置 `deleting` + 落台账 + 后台异步清理）。
         *
         *     需要 msg:manage 权限；`confirm` 缺失返回 10001；主题不存在 350001；**重复删除幂等受理**
         *     （返回已有台账，不产生二次副作用）。审计失败即请求失败（主题不会被置 `deleting`）。
         */
        delete: operations["delete_topic_api_v1_msg_admin_topics__topic_id__delete"];
        options?: never;
        head?: never;
        /**
         * Update Topic
         * @description 改保留期 / 容量上限（**部分更新**：只为给出的项写库，至少一项）。
         *
         *     需要 msg:manage 权限；主题不存在 350001，状态 `disabled` / `deleting` 时 350028，
         *     取值越界 350009。即时生效、不回溯删除已保留记录（后续清理按新值执行）。
         */
        patch: operations["update_topic_api_v1_msg_admin_topics__topic_id__patch"];
        trace?: never;
    };
    "/api/v1/msg/admin/topics/{topic_id}/delete-impact": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Topic Delete Impact
         * @description 删主题影响预检（二次确认弹窗的五项影响范围）。
         *
         *     需要 msg:manage 权限；主题不存在 350001。各维度独立容错：单项取数失败记 0 并告警，
         *     不因预检失败阻断后续删除（真正的保护在后端的状态与二次确认判定）。
         */
        get: operations["topic_delete_impact_api_v1_msg_admin_topics__topic_id__delete_impact_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/admin/topics/{topic_id}/partitions": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Expand Topic Partitions
         * @description 扩分区（**只增不减**：目标值不大于现状即 350009）。
         *
         *     需要 msg:manage 权限；主题不存在 350001，状态为 `disabled` / `deleting` 时 350028。
         *     占用现有 `TopicService.expand_partitions()`，不迁移既有 key 映射。
         */
        post: operations["expand_topic_partitions_api_v1_msg_admin_topics__topic_id__partitions_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/consume": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Consume
         * @description 拉取一批消息（成员制 + 分区内停等 + 长轮询 + inflight 双阈值）。
         *
         *     需要 msg:consume 权限；须先 `join` 并只在分配到的分区上拉取（组内不重复分配）。
         *     `acks=0` 表示不等确认（返回前位点直接前移，可能丢），同样受成员制约束。
         *     返回消息在分区内按 offset 升序；超时未 ack 会重投同一条消息（`delivery_count` 递增）；
         *     死信消息不再投递，其后的消息可继续（顺序性唯一破例）。
         *     主题不存在 350001，组不存在 350013，成员失效或分区不在其分配内 350014，
         *     分区号越界 350009，位点已被保留期清理 350003。
         *     入口先按主题保留口径清理本分区到期段（幂等；见 01-6），因此「位点早于起点」会在同一次请求里显式报 350003。
         *
         *     **长轮询（03-5）**：拉空后挂起到「有消息」或 `max_wait_ms` 到期；每轮都重跑成员校验
         *     （成员可能在挂起期间超时）。挂起数到全局上限时立即返空（不报错）。
         *     **inflight 阈值（03-5）**：已达阈值时不拉、直接返空 + `paused=true`；ack / nack / 死信归还后立即恢复。
         */
        post: operations["consume_api_v1_msg_consume_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/heartbeat": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Heartbeat
         * @description 成员心跳（独立于业务处理：处理慢不会因心跳断而被误判死亡）。
         *
         *     需要 msg:consume 权限；顺带清理同组超时成员，必要时重算分配并返回新 `generation`。
         *     组不存在 350013，成员不在组内或已超时失效 350014。
         */
        post: operations["heartbeat_api_v1_msg_heartbeat_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/join": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Join
         * @description 加入消费组（服务端主导分配）。
         *
         *     需要 msg:consume 权限；组按 `(namespace, topic)` 维度建，同 `consumer_id` 再次加入视为重连
         *     （刷新存活、不递增世代、分配不变）。返回 `generation` 与 `partitions`，客户端按分配拉取。
         *     主题不存在 350001，命名空间非已启用 module_key 返回 350010，组数配额已满返回 350026。
         */
        post: operations["join_api_v1_msg_join_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/leave": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Leave
         * @description 主动离开消费组（幂等：未注册成员也返回成功）。
         *
         *     需要 msg:consume 权限；离开后若组内仍有他人则立即重算分配，最后一名成员离开则组转 `empty`
         *     （组记录与位点保留）。组不存在 350013。
         */
        post: operations["leave_api_v1_msg_leave_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/nack": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Nack
         * @description 拒绝一条消息（转死信 + 位点前移；03-3 详细设计 §5.2）。
         *
         *     需要 msg:consume 权限（拿消息的人才能拒消息）；须是该分区的当前持有者（组不存在 350013，
         *     成员失效或分区已易主 350014）。位点越界 350002，消息不在投递中 350003。
         *     已死信的 offset 重复上报**幂等**（返回当前位点，不重复入队），因此本端点不声明 `350007`。
         */
        post: operations["nack_api_v1_msg_nack_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/publish": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Publish
         * @description 发布一条消息（缺省立即入日志；带 `delay_level` 则先进延迟区、到期才入日志）。
         *
         *     需要 msg:publish 权限；`tenant` 取自数据面解析链；主题不存在或未启用返回 350001，
         *     命名空间非已启用 module_key 返回 350010，载荷非合法 base64 返回 10001。
         *     延迟发布：`offset` 为 null、返回 `deliver_at` 与 `log_id`；档位非法 10001，延迟区已满 350015。
         *     首个延迟发布会惰性启动到期推进任务（无延迟消息的进程不起后台任务）。
         *     入口按主题保留口径清理本分区到期段（幂等；见 01-6）。
         *     **首次发布懒初始化配额缺省行**（05-2）；**限流在两条分支之前**（03-5 + 05-2）：
         *     延迟发布同样是生产，不限会让「延迟档」成为绕过限流的旁路；超出速率返回 350005（429 + `Retry-After`）。
         *     **存储配额在存储水位之前判**（05-2 / 02-6）：超租户存储硬限返回 350022（507，
         *     `data.reason=tenant_quota`）；盘满同样 350022（`data.reason=watermark`）。
         */
        post: operations["publish_api_v1_msg_publish_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/tenant-usage": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Tenant Usage
         * @description 返回当前租户的消息平台用量（四列 + 一行真实值）。
         *
         *     Args:
         *         usage: 用量快照服务。
         *         tenant: 当前租户。
         *
         *     Returns:
         *         JSONResponse: `{"columns": [...], "items": [row], "total": 1}`。
         */
        get: operations["tenant_usage_api_v1_msg_tenant_usage_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/tenant-usage/quota": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Quota Projection
         * @description 返回条数类配额投影（topic / group / unacked / connection）。
         *
         *     Args:
         *         quota: 配额服务。
         *         tenant: 当前租户。
         *         tenant_id: 查询参数租户主键（与上下文一致性由网关保证）。
         *
         *     Returns:
         *         JSONResponse: `{"items": [...]}`。
         */
        get: operations["quota_projection_api_v1_msg_tenant_usage_quota_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/tenant-usage/quota/bytes": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Quota Projection Bytes
         * @description 返回存储配额投影（storage_bytes）。
         *
         *     Args:
         *         quota: 配额服务。
         *         tenant: 当前租户。
         *         tenant_id: 查询参数租户主键。
         *
         *     Returns:
         *         JSONResponse: `{"items": [...]}`。
         */
        get: operations["quota_projection_bytes_api_v1_msg_tenant_usage_quota_bytes_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/api/v1/msg/tenant-usage/quota/rate": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Quota Projection Rate
         * @description 返回生产速率配额投影（produce_rate）。
         *
         *     Args:
         *         quota: 配额服务。
         *         tenant: 当前租户。
         *         tenant_id: 查询参数租户主键。
         *
         *     Returns:
         *         JSONResponse: `{"items": [...]}`。
         */
        get: operations["quota_projection_rate_api_v1_msg_tenant_usage_quota_rate_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/healthz": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Healthz
         * @description 存活检查端点。
         *
         *     Args:
         *         request: 当前请求（取服务身份）。
         *
         *     Returns:
         *         dict: 服务状态与身份，固定返回 {"status": "ok", "service": 名, "version": 版}。
         */
        get: operations["healthz_healthz_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/ack": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Ack
         * @description 确认一条消息（位点提交值 = `offset + 1`，提交失败不前移）。
         *
         *     需要 msg:consume 权限；须是该分区的当前持有者（组不存在 350013，成员失效或分区已易主 350014）。
         *     位点越界 350002，消息不在投递中 350003，消息已因重投超限转入死信 350007。
         */
        post: operations["ack_msg_ack_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/connection": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Connection
         * @description 回本连接元数据（协议版本 / 承载方式 / 客户端地址 + 连接键 / 活跃流 / 阈值 / drain 标记）。
         *
         *     只读自报，不含租户业务数据，故不加权限码（05 域认证落地后再评估收紧）。
         *     用途：排障第一问（协商到 h2 还是退回 1.1）、用例断言、客户端取服务端口径（心跳间隔 / 上限 / drain）。
         *     **兼作保活**：本端点每被调用一次就刷新连接 `last_seen`——无帧能力的客户端（curl / EventSource）
         *     用它当心跳，与 `POST /msg/connection/heartbeat` 共用同一份登记（03-4 详细设计 §6.1）。
         *     **不判连接数上限**（守卫豁免该判定）：它是打满时的排障入口，判上限会让「满」变成「连查都查不了」。
         *     版本可见（03-2）：`supported_protocol_version` 恒有值；本端点是 JSON 请求，`protocol_version` 恒为 null。
         *     **不声明 `x-error-codes`**：不触达业务判定，没有产品码可声明（与探针端点同性质）。
         */
        get: operations["connection_msg_connection_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/connection/heartbeat": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Heartbeat
         * @description 心跳：刷新连接保活并回服务端连接参数（帧 17 或 JSON，同一份 `ConnectionInfo`）。
         *
         *     无权限码：连接探活先于身份（05 域认证落地后再评估收紧）。
         *     帧形态：类型必须是 17、payload 必须为空、`request_id` 必填（缺失 `350018`）；主版本不符 `350019`。
         *     两种失败都以 200 + error 帧返回（帧可解析即协议内失败）。
         *     JSON 形态：空体，返回 `ApiResponse[ConnectionInfo]`；连接数超限时回 429（`350020`）。
         */
        post: operations["heartbeat_msg_connection_heartbeat_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/consume": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Consume
         * @description 拉取一批消息（成员制 + 分区内停等 + 长轮询 + inflight 双阈值）。
         *
         *     需要 msg:consume 权限；须先 `join` 并只在分配到的分区上拉取（组内不重复分配）。
         *     `acks=0` 表示不等确认（返回前位点直接前移，可能丢），同样受成员制约束。
         *     返回消息在分区内按 offset 升序；超时未 ack 会重投同一条消息（`delivery_count` 递增）；
         *     死信消息不再投递，其后的消息可继续（顺序性唯一破例）。
         *     主题不存在 350001，组不存在 350013，成员失效或分区不在其分配内 350014，
         *     分区号越界 350009，位点已被保留期清理 350003。
         *     入口先按主题保留口径清理本分区到期段（幂等；见 01-6），因此「位点早于起点」会在同一次请求里显式报 350003。
         *
         *     **长轮询（03-5）**：拉空后挂起到「有消息」或 `max_wait_ms` 到期；每轮都重跑成员校验
         *     （成员可能在挂起期间超时）。挂起数到全局上限时立即返空（不报错）。
         *     **inflight 阈值（03-5）**：已达阈值时不拉、直接返空 + `paused=true`；ack / nack / 死信归还后立即恢复。
         */
        post: operations["consume_msg_consume_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/nack": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Nack
         * @description 拒绝一条消息（转死信 + 位点前移；03-3 详细设计 §5.2）。
         *
         *     需要 msg:consume 权限（拿消息的人才能拒消息）；须是该分区的当前持有者（组不存在 350013，
         *     成员失效或分区已易主 350014）。位点越界 350002，消息不在投递中 350003。
         *     已死信的 offset 重复上报**幂等**（返回当前位点，不重复入队），因此本端点不声明 `350007`。
         */
        post: operations["nack_msg_nack_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/publish": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Publish
         * @description 发布一条消息（缺省立即入日志；带 `delay_level` 则先进延迟区、到期才入日志）。
         *
         *     需要 msg:publish 权限；`tenant` 取自数据面解析链；主题不存在或未启用返回 350001，
         *     命名空间非已启用 module_key 返回 350010，载荷非合法 base64 返回 10001。
         *     延迟发布：`offset` 为 null、返回 `deliver_at` 与 `log_id`；档位非法 10001，延迟区已满 350015。
         *     首个延迟发布会惰性启动到期推进任务（无延迟消息的进程不起后台任务）。
         *     入口按主题保留口径清理本分区到期段（幂等；见 01-6）。
         *     **首次发布懒初始化配额缺省行**（05-2）；**限流在两条分支之前**（03-5 + 05-2）：
         *     延迟发布同样是生产，不限会让「延迟档」成为绕过限流的旁路；超出速率返回 350005（429 + `Retry-After`）。
         *     **存储配额在存储水位之前判**（05-2 / 02-6）：超租户存储硬限返回 350022（507，
         *     `data.reason=tenant_quota`）；盘满同样 350022（`data.reason=watermark`）。
         */
        post: operations["publish_msg_publish_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/msg/subscribe": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Subscribe
         * @description 订阅一条分区（SSE 长驻流，server streaming）。
         *
         *     需要 msg:consume 权限；须先 `join` 且分区在该成员分配内（组不存在 350013，成员失效 350014）。
         *     预检与 `/msg/consume` 同口径：命名空间 350010、主题 350001、分区号 350009；
         *     `max_seconds` 越界（> 600）与其它参数形态问题由契约层拒绝（`10001`）。
         *     单连接长驻流超上限返回 350021（普通 JSON 错误，尚未进流；名额随即归还）。
         *     进流之后：拉到消息逐条 `message`；无消息按事件唤醒等待（保活期发注释行）；
         *     到期 `close(deadline)`；成员被撤销 / 分区易主 → `error`（同码）+ `close(member_lost)`。
         *     客户端断开或 `RST_STREAM` 后流被注销（`close(client_closed)`），已投未 ack 的消息仍可 ack。
         */
        get: operations["subscribe_msg_subscribe_get"];
        put?: never;
        /**
         * Subscribe Frames
         * @description 订阅一条分区（帧流形态，`application/x-msg-frame`）。
         *
         *     与 `GET /msg/subscribe` **同一套语义**（权限码 / 预检 / 背压 / 结束条件全同），差别只在承载：
         *     参数在帧 header、响应是连续帧。帧不可解析 / 类型不符 / 缺必填头 → `200` + 单帧 `error`（协议内失败）；
         *     预检失败同理（尚未进流，用 error 帧而**不是** JSON，客户端只按帧解析）。
         *     单连接长驻流超上限仍返回 350021（由连接守卫按帧形态给出）。
         */
        post: operations["subscribe_frames_msg_subscribe_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/readyz": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        /**
         * Readyz
         * @description 就绪检查端点（启动完成态 + 停机摘流 + 注册表聚合）。
         *
         *     Args:
         *         request: 当前请求（取应用启动完成态 / 停机摘流标记 / 服务身份）。
         *         registry: 健康检查项注册表（依赖注入）。
         *
         *     Returns:
         *         JSONResponse: 全部就绪 200；未就绪、启动未完成或停机中 503。
         */
        get: operations["readyz_readyz_get"];
        put?: never;
        post?: never;
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/rpc/{service}/{method}": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Invoke
         * @description 一元调用（JSON 形态）。
         *
         *     需要 `msg:rpc` 权限（当前无 provider 时恒放行）。`service` / `method` 形态非法由契约层拒绝
         *     （`10001`）；`payload` 非合法 base64 同码。未注册方法返回 `350023`（404，`layer=business`、
         *     `retryable=false`），不是 500。参数 / 结果超过 1 MiB 返回 `350006`。
         *     调用方标识与链路标识走「HTTP 头优先」，两者都会回写进响应头。
         *
         *     **预算**（04-2）：`deadline_ms` 缺省 30000、区间 0~600000（越界 `10001`）；耗尽返回 `350004`
         *     （504，`data.outcome=unknown`）。`used_ms` / `remaining_ms` 随响应头回传，供上游核对透传是否生效。
         *     取消（`RST_STREAM` / 连接断开）由 `ensure_not_cancelled()` 在写响应前复核，取消后不写响应。
         */
        post: operations["invoke_rpc__service___method__post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
    "/rpc/{service}/{method}/stream": {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        get?: never;
        put?: never;
        /**
         * Open Stream
         * @description 服务端流式调用（一次请求、多次响应；只接 `application/x-msg-frame`）。
         *
         *     需要 `msg:rpc` 权限（与一元同一权限码）。**只接帧形态**：JSON 请求按契约层拒绝（`10001`，
         *     基座统一错误响应），因为流式的响应是连续帧、没有 JSON 形态。
         *
         *     建流前的失败（帧不可解析 / 类型不符 / 置了响应位或结束位 / 缺 `request_id` / 压缩 / 租户不一致 /
         *     路径参数形态 / 预算已耗尽 / 配额 / 参数超限 / 方法未注册或非流式 / 长驻流名额满）一律返回
         *     **HTTP 200 + 单帧 `error`**；对应码为 `350018` / `350019` / `350012` / `10001` / `350004` /
         *     `350005` / `350006` / `350023` / `350021`。推流中的错误以 `ERROR` 帧收尾（`BizError` 原码、
         *     `350004` 预算、`350006` 单帧超限、`350024` 空闲超时）；正常结束由末帧 `end_of_stream` 表达；
         *     客户端取消不写收尾帧。
         *
         *     **路径参数不在契约里声明 `pattern` / `minLength`**（2026-11-06 确认）：FastAPI 的参数校验发生在
         *     端点之前，非法形态会先被渲染成 JSON 错误、永远到不了「单帧 `error`」这条口径；形态判据改在端点内
         *     用 `ensure_rpc_name`（与一元同一函数、同一码），契约里只保留可读描述。
         */
        post: operations["open_stream_rpc__service___method__stream_post"];
        delete?: never;
        options?: never;
        head?: never;
        patch?: never;
        trace?: never;
    };
}
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        /**
         * AckRequest
         * @description 确认请求（位点提交值 = `offset + 1`；成员制：分区须仍在该成员分配内）。
         */
        AckRequest: {
            /**
             * Consumer Id
             * @description 成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连）
             */
            consumer_id: string;
            /**
             * Group
             * @description 消费组名（组元数据与再均衡归 01-4）
             */
            group: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Offset
             * @description 待确认消息的分区内序号
             */
            offset: number;
            /**
             * Partition
             * @description 分区号（必带；组内分摊与再均衡归 01-4）
             */
            partition: number;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * AckResult
         * @description 确认结果（`committed_offset` 为下一条待消费的 offset）。
         */
        AckResult: {
            /**
             * Committed Offset
             * @description 已提交位点（已处理完的最后一条 + 1）
             */
            committed_offset: number;
        };
        /**
         * AclBatchRequest
         * @description 批量授予 / 更新请求（整批校验、整批成功 / 失败）。
         */
        AclBatchRequest: {
            /**
             * Items
             * @description 规则清单
             */
            items?: components["schemas"]["AclRuleRequest"][];
        };
        /**
         * AclBatchResult
         * @description 批量授予 / 更新结果。
         */
        AclBatchResult: {
            /**
             * Changed
             * @description 实际发生变更的规则 ID（幂等未变更为空）
             */
            changed?: string[];
            /**
             * Items
             * @description 写入后的规则（与入参同序）
             */
            items?: components["schemas"]["AclItem"][];
        };
        /**
         * AclItem
         * @description ACL 规则明细。
         */
        AclItem: {
            /**
             * Acl Id
             * @description 规则 ID（雪花，JSON 输出为字符串）
             */
            acl_id: string;
            /**
             * Action
             * @description 动作
             */
            action: string;
            /**
             * Effect
             * @description 效果
             */
            effect: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Principal Key
             * @description 主体标识
             */
            principal_key: string;
            /**
             * Principal Type
             * @description 主体类型
             */
            principal_type: string;
            /**
             * Status
             * @description 状态（enabled / disabled）
             */
            status: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Updated At
             * @description 最近更新时间（UTC）
             */
            updated_at: string | null;
        };
        /**
         * AclPage
         * @description ACL 列表响应（页码分页）。
         */
        AclPage: {
            /**
             * Items
             * @description 当页规则
             */
            items?: components["schemas"]["AclItem"][];
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 符合条件的总数
             */
            total: number;
        };
        /**
         * AclRevokeResult
         * @description 收回结果（幂等）。
         */
        AclRevokeResult: {
            /**
             * Acl Id
             * @description 规则 ID（雪花，JSON 输出为字符串）
             */
            acl_id: string;
            /**
             * Changed
             * @description 是否实际发生停用（已停用 / 不存在为 false）
             */
            changed: boolean;
        };
        /**
         * AclRuleRequest
         * @description 一条 ACL 规则（授予 / 更新）。
         */
        AclRuleRequest: {
            /**
             * Action
             * @description 动作（publish / subscribe / describe / manage）
             */
            action: string;
            /**
             * Effect
             * @description 效果（allow / deny）
             */
            effect: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key）
             */
            namespace: string;
            /**
             * Principal Key
             * @description 主体标识（用户 / 角色主键的十进制字符串）
             */
            principal_key: string;
            /**
             * Principal Type
             * @description 主体类型（user / role）
             */
            principal_type: string;
            /**
             * Topic
             * @description 主题名（须已存在且属于当前租户）
             */
            topic: string;
        };
        /**
         * AffectedGroupItem
         * @description 受影响消费组的影响视图（基线 / 当前 / 变化）。
         *
         *     口径与 01-4 组详情同源：`lag = leo - committed`，**不夹取负值**；度量固定按基线时刻的分区集合，
         *     主题事后扩分区不影响可比性。
         */
        AffectedGroupItem: {
            /**
             * Baseline Lag
             * @description 创建任务时的组级积压（基线）
             */
            baseline_lag: number;
            /**
             * Captured At
             * Format: date-time
             * @description 该值所属时刻（UTC；未终结为基线时刻，已终结为快照时刻）
             */
            captured_at: string;
            /**
             * Current Lag
             * @description 当前组级积压（同一分区集合；可负）
             */
            current_lag: number;
            /**
             * Delta
             * @description 变化量（当前 - 基线；可负）
             */
            delta: number;
            /**
             * Group
             * @description 消费组名
             */
            group: string;
        };
        /**
         * ApiResponse
         * @description 统一响应体：`code=0` 成功，非 0 业务错误码。
         *
         *     - `data` 为业务数据（泛型）；失败时为 `null`；分页载荷复用分页契约基类。
         *     - 雪花 ID 在 JSON 中以字符串输出（`BaseSchema` 统一序列化口径）。
         */
        ApiResponse: {
            /**
             * Code
             * @default 0
             */
            code: number;
            /** Data */
            data?: unknown | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[AckResult] */
        ApiResponse_AckResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["AckResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[AclBatchResult] */
        ApiResponse_AclBatchResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["AclBatchResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[AclPage] */
        ApiResponse_AclPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["AclPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[AclRevokeResult] */
        ApiResponse_AclRevokeResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["AclRevokeResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[ConnectionInfo] */
        ApiResponse_ConnectionInfo_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["ConnectionInfo"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[ConsumeResult] */
        ApiResponse_ConsumeResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["ConsumeResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DelaySummary] */
        ApiResponse_DelaySummary_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DelaySummary"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DelayedPage] */
        ApiResponse_DelayedPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DelayedPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DeleteImpact] */
        ApiResponse_DeleteImpact_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DeleteImpact"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DlqDetail] */
        ApiResponse_DlqDetail_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DlqDetail"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DlqDisposeResult] */
        ApiResponse_DlqDisposeResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DlqDisposeResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[DlqList] */
        ApiResponse_DlqList_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["DlqList"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[GroupDetail] */
        ApiResponse_GroupDetail_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["GroupDetail"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[GroupPage] */
        ApiResponse_GroupPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["GroupPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[LeaveResult] */
        ApiResponse_LeaveResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["LeaveResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[MemberAssignment] */
        ApiResponse_MemberAssignment_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["MemberAssignment"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[MessagePage] */
        ApiResponse_MessagePage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["MessagePage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[NackResult] */
        ApiResponse_NackResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["NackResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[OffsetResetResult] */
        ApiResponse_OffsetResetResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["OffsetResetResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[Overview] */
        ApiResponse_Overview_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["Overview"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[PartitionPage] */
        ApiResponse_PartitionPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["PartitionPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[PublishResult] */
        ApiResponse_PublishResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["PublishResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[QuotaView] */
        ApiResponse_QuotaView_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["QuotaView"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[ReplayTaskItem] */
        ApiResponse_ReplayTaskItem_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["ReplayTaskItem"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[ReplayTaskPage] */
        ApiResponse_ReplayTaskPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["ReplayTaskPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[RpcResult] */
        ApiResponse_RpcResult_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["RpcResult"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[TopicCleanupView] */
        ApiResponse_TopicCleanupView_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["TopicCleanupView"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[TopicDetail] */
        ApiResponse_TopicDetail_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["TopicDetail"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[TopicItem] */
        ApiResponse_TopicItem_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["TopicItem"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /** ApiResponse[TopicPage] */
        ApiResponse_TopicPage_: {
            /**
             * Code
             * @default 0
             */
            code: number;
            data?: components["schemas"]["TopicPage"] | null;
            /**
             * Message
             * @default ok
             */
            message: string;
        };
        /**
         * CleanupProgress
         * @description 删主题清理进度（`deleting` 主题的详情里出现；其余状态为 null）。
         */
        CleanupProgress: {
            /**
             * Attempts
             * @description 已执行轮次（连续失败 ≥3 会输出告警日志）
             */
            attempts: number;
            /**
             * Done Steps
             * @description 已完成步骤键（messages / delay / offsets / groups / dlq / runtime / metadata）
             */
            done_steps?: string[];
            /**
             * Finished At
             * @description 完成时刻（UTC；未完成为 null）
             */
            finished_at: string | null;
            /**
             * Last Error
             * @description 最后一轮失败原因（未失败为 null）
             */
            last_error: string | null;
            /**
             * Started At
             * @description 首轮开始时刻（UTC；未开始为 null）
             */
            started_at: string | null;
            /**
             * Status
             * @description 台账状态（pending / running / completed）
             */
            status: string;
        };
        /**
         * ConnectionInfo
         * @description 连接元数据（`GET /msg/connection`、`POST /msg/connection/heartbeat`、心跳帧三处共用）。
         *
         *     一次把客户端自适应需要的参数给全（间隔 / 超时 / 两个上限 / drain），SDK 不必再查常量
         *     （03-4 详细设计 §6.2）。字段**只增不删**：新增仍走本模型，客户端按未知字段忽略处理。
         */
        ConnectionInfo: {
            /**
             * Active Streams
             * @description 该连接上的长驻流数
             * @default 0
             */
            active_streams: number;
            /**
             * Client Host
             * @description 客户端地址（无地址场景为空串）
             */
            client_host: string;
            /**
             * Client Port
             * @description 客户端端口（无地址场景为 0）
             */
            client_port: number;
            /**
             * Connection Key
             * @description 派生连接键（tenant|host:port；与日志同值）
             * @default
             */
            connection_key: string;
            /**
             * Draining
             * @description 本实例是否在摘流（true 时客户端应换实例重连）
             * @default false
             */
            draining: boolean;
            /**
             * Heartbeat Interval Ms
             * @description 服务端建议心跳间隔（毫秒）
             * @default 0
             */
            heartbeat_interval_ms: number;
            /**
             * Heartbeat Timeout Ms
             * @description 服务端判定半开的阈值（毫秒）
             * @default 0
             */
            heartbeat_timeout_ms: number;
            /**
             * Http Version
             * @description 协商到的 HTTP 版本（HTTP/2 / HTTP/1.1）
             */
            http_version: string;
            /**
             * Idle Ms
             * @description 距最近一次请求的时长（毫秒；判半开的可见值）
             * @default 0
             */
            idle_ms: number;
            /**
             * Idle Timeout Ms
             * @description 连接名额回收阈值（毫秒）
             * @default 0
             */
            idle_timeout_ms: number;
            /**
             * Is Tls
             * @description 是否 TLS 承载（TLS 下由 ALPN 协商 h2）
             */
            is_tls: boolean;
            /**
             * Last Heartbeat At
             * @description 最近一次心跳时刻；从未心跳为 null
             */
            last_heartbeat_at?: string | null;
            /**
             * Max Connections
             * @description 单副本连接数上限（超限 350020）
             * @default 0
             */
            max_connections: number;
            /**
             * Max Streams Per Connection
             * @description 单连接长驻流上限（超限 350021）
             * @default 0
             */
            max_streams_per_connection: number;
            /**
             * Protocol Version
             * @description 本请求所用帧协议版本（帧形态非空；JSON 请求为 null）
             */
            protocol_version?: string | null;
            /**
             * Requests
             * @description 该连接累计请求数
             * @default 0
             */
            requests: number;
            /**
             * Retry After Ms
             * @description 建议重试间隔（毫秒；drain / 限流时用）
             * @default 0
             */
            retry_after_ms: number;
            /**
             * Scheme
             * @description 承载协议（http / https）
             */
            scheme: string;
            /**
             * Server Time
             * Format: date-time
             * @description 服务端当前时间
             */
            server_time?: string;
            /**
             * Supported Protocol Version
             * @description 服务端当前帧协议版本（恒有值）
             * @default 1.0
             */
            supported_protocol_version: string;
            /**
             * Tenant
             * @description 租户编码；无租户上下文为 anonymous
             * @default anonymous
             */
            tenant: string;
            /**
             * Uptime Ms
             * @description 连接存活时长（毫秒）
             * @default 0
             */
            uptime_ms: number;
        };
        /**
         * ConsumeRequest
         * @description 拉取请求（成员制：须先 `join`，且 `partition` 必须在该成员的分配内）。
         */
        ConsumeRequest: {
            /**
             * Acks
             * @description 投递档位：1 = 确认后才算成功（至少一次，缺省）；0 = 不等确认（可能丢）
             * @default 1
             */
            acks: number;
            /**
             * Consumer Id
             * @description 成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连）
             */
            consumer_id: string;
            /**
             * Group
             * @description 消费组名（组元数据与再均衡归 01-4）
             */
            group: string;
            /**
             * Max Messages
             * @description 本次最多拉取条数（1~100）
             * @default 20
             */
            max_messages: number;
            /**
             * Max Wait Ms
             * @description 长轮询挂起上限（毫秒；0 = 不挂起立即返回；越界 10001）
             * @default 30000
             */
            max_wait_ms: number;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Partition
             * @description 分区号（必带；组内分摊与再均衡归 01-4）
             */
            partition: number;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * ConsumeResult
         * @description 拉取结果（可能为空：位点已到 LEO、剩余消息均在可见性窗口内，或 inflight 已达阈值）。
         *
         *     `paused` / 两个用量字段是 03-5 的背压自报：**暂停不是错误**，客户端据此退避（或按 `max_wait_ms`
         *     继续等），而不是按错误码重试。
         */
        ConsumeResult: {
            /**
             * Inflight Bytes
             * @description 本分区当前在途字节合计（按 payload 线上字节）
             * @default 0
             */
            inflight_bytes: number;
            /**
             * Inflight Messages
             * @description 本分区当前在途条数（已投出未确认）
             * @default 0
             */
            inflight_messages: number;
            /**
             * Messages
             * @description 本次投递的消息（按 offset 升序）
             */
            messages?: components["schemas"]["bms_msg__schemas__message__MessageItem"][];
            /**
             * Paused
             * @description 是否因 inflight 达阈值而暂停推送（不是错误）
             * @default false
             */
            paused: boolean;
        };
        /**
         * DelayLevel
         * @description 延迟档位（固定 9 档；与 `core/config.DELAY_LEVEL_MS` 一一对应，非法值由契约层拒绝）。
         *
         *     不做任意精度：档位是容量与可测性的边界（任意毫秒会让时间轮与用例同时失控）。
         * @enum {string}
         */
        DelayLevel: "1s" | "5s" | "10s" | "30s" | "1m" | "5m" | "10m" | "30m" | "60m";
        /**
         * DelaySummary
         * @description 延迟汇总（与 `/admin/delayed` 同一份当前租户快照）。
         */
        DelaySummary: {
            /**
             * Hint
             * @description 结构化堆积提示：normal / stale / near_quota
             */
            hint: string;
            /**
             * Levels
             * @description 九档分布（按档位声明序）
             */
            levels: components["schemas"]["LevelBucket"][];
            /**
             * Namespace
             * @description 命名空间过滤值
             */
            namespace: string | null;
            /**
             * Quota Used Ratio
             * @description 当前租户待投量 / 软上限（夹到 0~1）
             */
            quota_used_ratio: number;
            /**
             * Stale Count
             * @description 已到期但未转投的条数
             */
            stale_count: number;
            /**
             * Topic
             * @description 主题过滤值
             */
            topic: string | null;
            /**
             * Total
             * @description 过滤后待投总条数
             */
            total: number;
        };
        /**
         * DelayedItem
         * @description 一条延迟中消息（尚未进入日志：没有 offset，只有到期时刻）。
         */
        DelayedItem: {
            /**
             * Deliver At
             * Format: date-time
             * @description 到期时刻（UTC）
             */
            deliver_at: string;
            /**
             * Level
             * @description 延迟档位（1s / 5s / 10s / 30s / 1m / 5m / 10m / 30m / 60m）
             */
            level: string;
            /**
             * Log Id
             * @description 记录 ID（发布时分配；转投沿用，审计条目可凭它定位）
             */
            log_id: string;
            /**
             * Message Id
             * @description 消息 ID（重投 / 转投 / 重放不变）
             */
            message_id: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Partition
             * @description 发布时锁定的分区号（扩分区后不漂移）
             */
            partition: number;
            /**
             * Remaining Ms
             * @description 剩余时间（毫秒；已到期未转投为 0）
             */
            remaining_ms: number;
            /**
             * Topic
             * @description 主题名（主题删除后仍可展示，便于排障）
             */
            topic: string;
        };
        /**
         * DelayedPage
         * @description 待投列表分页（`total` 为过滤后总数）。
         */
        DelayedPage: {
            /**
             * Items
             * @description 当页延迟消息（到期时刻升序，最急的在前）
             */
            items: components["schemas"]["DelayedItem"][];
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 过滤后总数
             */
            total: number;
        };
        /**
         * DeleteImpact
         * @description 删主题影响预检（二次确认弹窗的「影响范围」唯一数据源）。
         */
        DeleteImpact: {
            /**
             * Delayed Total
             * @description 延迟待投条数
             */
            delayed_total: number;
            /**
             * Dlq Total
             * @description 死信条数
             */
            dlq_total: number;
            /**
             * Group Count
             * @description 该主题的消费组数
             */
            group_count: number;
            /**
             * Message Total
             * @description 可读消息总量（Σ(LEO − 起点)）
             */
            message_total: number;
            /**
             * Partition Count
             * @description 分区数
             */
            partition_count: number;
        };
        /**
         * DlqArchiveRequest
         * @description 归档请求（原因可选）。
         */
        DlqArchiveRequest: {
            /**
             * Confirm
             * @description 显式确认（高危写操作；必须为 true）
             * @default false
             */
            confirm: boolean;
            /**
             * Reason
             * @description 归档原因（可选）
             */
            reason?: string | null;
        };
        /**
         * DlqDetail
         * @description 死信详情（摘要 + 失败上下文）。
         */
        DlqDetail: {
            /**
             * Attempts
             * @description 累计投递次数
             */
            attempts: number;
            /**
             * Created At
             * Format: date-time
             * @description 落库时间（UTC）
             */
            created_at: string;
            /**
             * Detail
             * @description 失败上下文（字段名 → 说明；消费端上报时原样保存）
             */
            detail?: {
                [key: string]: string;
            };
            /**
             * Disposed At
             * @description 处置时间（UTC）
             */
            disposed_at?: string | null;
            /**
             * Disposition Reason
             * @description 处置原因
             */
            disposition_reason?: string | null;
            /**
             * Failed At
             * Format: date-time
             * @description 失败时间（UTC）
             */
            failed_at: string;
            /**
             * Id
             * @description 死信 ID（雪花，JSON 输出为字符串）
             */
            id: string;
            /**
             * Log Id
             * @description 原记录 ID
             */
            log_id: string;
            /**
             * Message Id
             * @description 原消息 ID
             */
            message_id: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Offset
             * @description 原 offset（分区内序号）
             */
            offset: number;
            /**
             * Partition No
             * @description 分区号
             */
            partition_no: number;
            /**
             * Reason
             * @description 转入原因（delivery_exhausted / schema_invalid / processing_failed）
             */
            reason: string;
            /**
             * Replay Task Id
             * @description 重放任务 ID（仅 replaying 有值）
             */
            replay_task_id?: string | null;
            /**
             * Status
             * @description 状态（pending / replaying / resolved / ignored / archived）
             */
            status: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Trace Id
             * @description 链路标识
             */
            trace_id?: string | null;
        };
        /**
         * DlqDisposeResult
         * @description 处置响应（状态与重放任务 ID）。
         */
        DlqDisposeResult: {
            /**
             * Id
             * @description 死信 ID
             */
            id: string;
            /**
             * Replay Task Id
             * @description 重放任务 ID（仅重放返回）
             */
            replay_task_id?: string | null;
            /**
             * Status
             * @description 处置后的状态
             */
            status: string;
        };
        /**
         * DlqIgnoreRequest
         * @description 忽略请求（原因必填、去空白后 1~256）。
         */
        DlqIgnoreRequest: {
            /**
             * Confirm
             * @description 显式确认（高危写操作；必须为 true）
             * @default false
             */
            confirm: boolean;
            /**
             * Reason
             * @description 忽略原因（必填）
             */
            reason: string;
        };
        /**
         * DlqItem
         * @description 死信摘要（列表行）。
         */
        DlqItem: {
            /**
             * Attempts
             * @description 累计投递次数
             */
            attempts: number;
            /**
             * Created At
             * Format: date-time
             * @description 落库时间（UTC）
             */
            created_at: string;
            /**
             * Disposed At
             * @description 处置时间（UTC）
             */
            disposed_at?: string | null;
            /**
             * Disposition Reason
             * @description 处置原因
             */
            disposition_reason?: string | null;
            /**
             * Failed At
             * Format: date-time
             * @description 失败时间（UTC）
             */
            failed_at: string;
            /**
             * Id
             * @description 死信 ID（雪花，JSON 输出为字符串）
             */
            id: string;
            /**
             * Log Id
             * @description 原记录 ID
             */
            log_id: string;
            /**
             * Message Id
             * @description 原消息 ID
             */
            message_id: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Offset
             * @description 原 offset（分区内序号）
             */
            offset: number;
            /**
             * Partition No
             * @description 分区号
             */
            partition_no: number;
            /**
             * Reason
             * @description 转入原因（delivery_exhausted / schema_invalid / processing_failed）
             */
            reason: string;
            /**
             * Replay Task Id
             * @description 重放任务 ID（仅 replaying 有值）
             */
            replay_task_id?: string | null;
            /**
             * Status
             * @description 状态（pending / replaying / resolved / ignored / archived）
             */
            status: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Trace Id
             * @description 链路标识
             */
            trace_id?: string | null;
        };
        /**
         * DlqList
         * @description 死信分页响应。
         */
        DlqList: {
            /**
             * Items
             * @description 当页死信
             */
            items: components["schemas"]["DlqItem"][];
            /**
             * Page
             * @description 页码
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 过滤后总数
             */
            total: number;
        };
        /**
         * DlqReplayRequest
         * @description 重放请求（`confirm` 必填 true；速率缺省 100 条/秒）。
         */
        DlqReplayRequest: {
            /**
             * Confirm
             * @description 显式确认（高危写操作；必须为 true）
             * @default false
             */
            confirm: boolean;
            /**
             * Rate Per Second
             * @description 重放限速（条/秒，1~1000）
             * @default 100
             */
            rate_per_second: number;
        };
        /**
         * GroupDetail
         * @description 组详情（状态 / 世代 / 成员 / 分配 / 各分区位点与积压）。
         */
        GroupDetail: {
            /**
             * Generation
             * @description 世代号
             */
            generation: number;
            /**
             * Group
             * @description 消费组名
             */
            group: string;
            /**
             * Members
             * @description 成员列表（按 consumer_id 升序）
             */
            members: components["schemas"]["GroupMemberItem"][];
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Partition Count
             * @description 主题分区数
             */
            partition_count: number;
            /**
             * Partitions
             * @description 各分区位点与积压（按分区号升序）
             */
            partitions: components["schemas"]["GroupPartitionItem"][];
            /**
             * State
             * @description 组状态（empty / preparing_rebalance / stable / dead）
             */
            state: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * GroupItem
         * @description 组列表行（管理面只读视图；积压只进详情）。
         */
        GroupItem: {
            /**
             * Assigned Partitions
             * @description 全组已分配的分区总数
             */
            assigned_partitions: number;
            /**
             * Generation
             * @description 世代号
             */
            generation: number;
            /**
             * Group
             * @description 消费组名
             */
            group: string;
            /**
             * Member Count
             * @description 活跃成员数
             */
            member_count: number;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * State
             * @description 组状态（empty / preparing_rebalance / stable / dead）
             */
            state: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * GroupMemberItem
         * @description 组成员明细。
         */
        GroupMemberItem: {
            /**
             * Consumer Id
             * @description 成员标识
             */
            consumer_id: string;
            /**
             * Joined At
             * Format: date-time
             * @description 加入时间（UTC；重连不刷新）
             */
            joined_at: string;
            /**
             * Last Seen
             * Format: date-time
             * @description 最后心跳时间（UTC）
             */
            last_seen: string;
            /**
             * Partitions
             * @description 当前持有的分区号（升序）
             */
            partitions: number[];
        };
        /**
         * GroupPage
         * @description 组列表分页（`total` 为过滤后总数）。
         */
        GroupPage: {
            /**
             * Items
             * @description 当页组（组名升序）
             */
            items: components["schemas"]["GroupItem"][];
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 过滤后总数
             */
            total: number;
        };
        /**
         * GroupPartitionItem
         * @description 组内分区位点与积压。
         */
        GroupPartitionItem: {
            /**
             * Committed
             * @description 已提交位点（下一条待消费的 offset）
             */
            committed: number;
            /**
             * Lag
             * @description 积压（LEO - 已提交位点）
             */
            lag: number;
            /**
             * Leo
             * @description 分区日志末端位点
             */
            leo: number;
            /**
             * Owner
             * @description 当前持有者（无成员或未分配为 null）
             */
            owner: string | null;
            /**
             * Partition
             * @description 分区号
             */
            partition: number;
        };
        /** HTTPValidationError */
        HTTPValidationError: {
            /** Detail */
            detail?: components["schemas"]["ValidationError"][];
        };
        /**
         * LeaveResult
         * @description 离开结果（`leave` 幂等：未注册成员也返回成功）。
         */
        LeaveResult: {
            /**
             * Consumer Id
             * @description 成员标识
             */
            consumer_id: string;
            /**
             * Generation
             * @description 离开后的世代号
             */
            generation: number;
            /**
             * Group
             * @description 消费组名
             */
            group: string;
            /**
             * State
             * @description 离开后的组状态（无成员即 empty）
             */
            state: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * LevelBucket
         * @description 一个延迟档位的汇总。
         */
        LevelBucket: {
            /**
             * Count
             * @description 该档待投条数（无记录也输出 0）
             */
            count: number;
            /**
             * Level
             * @description 延迟档位（1s / 5s / … / 60m）
             */
            level: string;
            /**
             * Min Remaining Ms
             * @description 该档最短剩余毫秒数；无记录为 0
             */
            min_remaining_ms: number;
        };
        /**
         * MemberAssignment
         * @description 成员分配（`join` / `heartbeat` 响应：客户端按 `partitions` 拉取，`generation` 变了即换分配）。
         */
        MemberAssignment: {
            /**
             * Consumer Id
             * @description 成员标识
             */
            consumer_id: string;
            /**
             * Generation
             * @description 世代号（成员集合每变化一次 +1；重连不递增）
             */
            generation: number;
            /**
             * Group
             * @description 消费组名
             */
            group: string;
            /**
             * Heartbeat Interval Ms
             * @description 建议心跳间隔（毫秒；服务端声明，客户端据此保活）
             */
            heartbeat_interval_ms: number;
            /**
             * Member Count
             * @description 组内当前活跃成员数
             */
            member_count: number;
            /**
             * Partitions
             * @description 本成员当前持有的分区号（升序；空闲成员为空数组）
             */
            partitions: number[];
            /**
             * Session Timeout Ms
             * @description 会话超时（毫秒；超时未心跳即判定成员失效）
             */
            session_timeout_ms: number;
            /**
             * State
             * @description 组状态（empty / preparing_rebalance / stable / dead）
             */
            state: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * MemberRequest
         * @description 成员请求（`join` / `heartbeat` / `leave` 共用：组按主题维度建）。
         */
        MemberRequest: {
            /**
             * Consumer Id
             * @description 成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连）
             */
            consumer_id: string;
            /**
             * Group
             * @description 消费组名（组元数据与再均衡归 01-4）
             */
            group: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * MessagePage
         * @description 消息检索分页（`total` 为过滤后总数）。
         */
        MessagePage: {
            /**
             * Items
             * @description 当页消息头（按分区号、offset 升序）
             */
            items: components["schemas"]["bms_msg__schemas__admin_view__MessageItem"][];
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Total
             * @description 过滤后总数
             */
            total: number;
        };
        /**
         * NackReason
         * @description 拒绝原因（消费端上报；`delivery_exhausted` 由 broker 独占，不接受外部指定）。
         * @enum {string}
         */
        NackReason: "schema_invalid" | "processing_failed";
        /**
         * NackRequest
         * @description 拒绝一条消息（转死信 + 位点前移；03-3 详细设计 §5.2）。
         *
         *     成员制与 `ack` 同口径：组须存在、成员须活跃、分区须在其分配内。
         */
        NackRequest: {
            /**
             * Consumer Id
             * @description 成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连）
             */
            consumer_id: string;
            /**
             * Detail
             * @description 失败详情（字段名 → 说明，如 field / expected / actual）
             */
            detail?: {
                [key: string]: string;
            };
            /**
             * Group
             * @description 消费组名（组元数据与再均衡归 01-4）
             */
            group: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Offset
             * @description 待拒绝消息的分区内序号
             */
            offset: number;
            /**
             * Partition
             * @description 分区号（必带；组内分摊与再均衡归 01-4）
             */
            partition: number;
            /** @description 拒绝原因（schema_invalid / processing_failed） */
            reason: components["schemas"]["NackReason"];
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * NackResult
         * @description 拒绝结果（与 `AckResult` 同口径：`committed_offset` 为下一条待消费的 offset）。
         */
        NackResult: {
            /**
             * Committed Offset
             * @description 已提交位点（被拒消息之后的第一条）
             */
            committed_offset: number;
        };
        /**
         * OffsetResetRequest
         * @description 位点重置请求。
         */
        OffsetResetRequest: {
            /**
             * Confirm
             * @description 显式确认（必须为 true；缺省 / false 一律拒绝，防脚本绕过）
             */
            confirm: boolean;
            /**
             * Group
             * @description 消费组名（组元数据与再均衡归 01-4）
             */
            group: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Offset
             * @description 目标位点（不早于分区起点，且不超过 LEO）
             */
            offset: number;
            /**
             * Partition
             * @description 分区号（必带；组内分摊与再均衡归 01-4）
             */
            partition: number;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * OffsetResetResult
         * @description 位点重置结果（含重置前值，便于前端与审计对账）。
         */
        OffsetResetResult: {
            /**
             * Group
             * @description 消费组名
             */
            group: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Offset
             * @description 重置后位点
             */
            offset: number;
            /**
             * Partition
             * @description 分区号
             */
            partition: number;
            /**
             * Previous Offset
             * @description 重置前位点（该组该分区无已提交位点时为 0）
             */
            previous_offset: number;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * Overview
         * @description 平台概览四张数字卡 + 快照可用性。
         */
        Overview: {
            /**
             * Consumer Lag
             * @description 当前租户积压条数
             */
            consumer_lag: number;
            /**
             * Disk Used Ratio
             * @description 磁盘使用率（多卷取最大值）
             */
            disk_used_ratio: number;
            /**
             * Dlq Total
             * @description 当前租户死信存量
             */
            dlq_total: number;
            /**
             * Messages In Per Second
             * @description 入口吞吐（本采样周期条数 / 周期秒数）
             */
            messages_in_per_second: number;
            /**
             * Snapshot Available
             * @description 采样快照是否可用；进程刚启动时为 false，四值返回 0
             */
            snapshot_available: boolean;
        };
        /**
         * PartitionPage
         * @description 分区详情响应。
         */
        PartitionPage: {
            /**
             * Group
             * @description 消费组；未指定时为 null
             */
            group: string | null;
            /**
             * Items
             * @description 分区明细（按分区号升序）
             */
            items: components["schemas"]["bms_msg__schemas__admin_view__PartitionItem"][];
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Partition Count
             * @description 主题分区总数
             */
            partition_count: number;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * PublishRequest
         * @description 发布请求（`tenant` 不在请求体：由数据面解析链注入，防客户端自报冒充）。
         */
        PublishRequest: {
            /**
             * Compression
             * @description 压缩算法（缺省 none；帧路径由 flags.compressed 与它共同判定，两者必须自洽）
             */
            compression?: string | null;
            /**
             * Content Type
             * @description 消息体内容类型（缺省按 application/msgpack；见协议契约 payload_content_types）
             */
            content_type?: string | null;
            /** @description 延迟档位（缺省 null = 普通发布；给值即延迟发布，到期才进入 offset 空间） */
            delay_level?: components["schemas"]["DelayLevel"] | null;
            /**
             * Headers
             * @description 结构化头（trace / 业务标记）
             */
            headers?: {
                [key: string]: string;
            };
            /**
             * Key
             * @description 分区键（同 key 恒同分区）
             */
            key?: string | null;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Parent Span Id
             * @description 生产 span 的父 span（链路起点可为空）
             */
            parent_span_id?: string | null;
            /**
             * Payload
             * @description 消息体（base64 编码；空串表示空消息体）
             * @default
             */
            payload: string;
            /**
             * Span Id
             * @description 生产者本次 span 标识（消费端据它建子 span）
             */
            span_id?: string | null;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
            /**
             * Trace Flags
             * @description 采样标记（约定 W3C 8 位十六进制；broker 不解释，原样透传）
             */
            trace_flags?: string | null;
            /**
             * Trace Id
             * @description 链路标识
             */
            trace_id?: string | null;
            /**
             * Uncompressed Len
             * @description 解压后字节数（可选声明；broker 只透传，不参与上限判定）
             */
            uncompressed_len?: number | null;
        };
        /**
         * PublishResult
         * @description 发布结果（`message_id` 与 `offset` 解耦：前者全局唯一，后者分区内序号）。
         */
        PublishResult: {
            /**
             * Deliver At
             * @description 到期时刻（UTC；普通发布为 null）
             */
            deliver_at: string | null;
            /**
             * Log Id
             * @description 记录 ID（发布即分配；延迟发布到期转投沿用，审计可凭它定位记录）
             */
            log_id: string;
            /**
             * Message Id
             * @description 消息 ID（雪花，JSON 输出为字符串；重投 / 重放不变）
             */
            message_id: string;
            /**
             * Offset
             * @description 分区内序号（延迟发布尚未进入 offset 空间，为 null）
             */
            offset: number | null;
            /**
             * Partition
             * @description 落到的分区号
             */
            partition: number;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
        };
        /**
         * QuotaItem
         * @description 一维配额的管理面视图。
         */
        QuotaItem: {
            /**
             * Hard Limit
             * @description 硬限（与 limit 同值，平台投影字段口径）
             */
            hard_limit: number | null;
            /**
             * Limit
             * @description 硬限（null = 不限）
             */
            limit: number | null;
            /**
             * Namespace
             * @description 作用域（空串 = 租户级）
             */
            namespace: string;
            /**
             * Period
             * @description 周期（none）
             */
            period: string;
            /**
             * Quota Key
             * @description 配额维度键
             */
            quota_key: string;
            /**
             * Soft Limit
             * @description 生效软限（null = 无软限）
             */
            soft_limit: number | null;
            /**
             * Source
             * @description 来源（default / configured）
             */
            source: string;
            /**
             * State
             * @description 状态（unconfigured / ok / soft_exceeded / hard_exceeded / unavailable）
             */
            state: string;
            /**
             * Unit
             * @description 单位（count / bytes / qps）
             */
            unit: string;
            /**
             * Updated At
             * @description 最近更新时间（UTC；缺省行为 null）
             */
            updated_at: string | null;
            /**
             * Usage
             * @description 当前用量（null = 不可用）
             */
            usage: number | null;
        };
        /**
         * QuotaUpdateItem
         * @description 一条配额调整。
         */
        QuotaUpdateItem: {
            /**
             * Limit
             * @description 硬限（0 = 不限）
             */
            limit: number;
            /**
             * Namespace
             * @description 作用域（空串 = 租户级）
             * @default
             */
            namespace: string;
            /**
             * Quota Key
             * @description 配额维度键
             */
            quota_key: string;
            /**
             * Soft Limit
             * @description 软限（null / 0 = 未设置；不得大于硬限）
             */
            soft_limit?: number | null;
        };
        /**
         * QuotaUpdateRequest
         * @description 批量调整请求（整批校验、整批失败）。
         */
        QuotaUpdateRequest: {
            /**
             * Items
             * @description 调整项
             */
            items?: components["schemas"]["QuotaUpdateItem"][];
        };
        /**
         * QuotaView
         * @description 配额查询响应。
         */
        QuotaView: {
            /**
             * Items
             * @description 六维配额（固定顺序）
             */
            items?: components["schemas"]["QuotaItem"][];
        };
        /**
         * ReplayRequest
         * @description 建重放任务请求（`tenant` 不在请求体：由基座解析链注入）。
         */
        ReplayRequest: {
            /**
             * Confirm
             * @description 显式确认（批量重放属高危写操作；必须为 true，缺省 / false 一律拒绝，防脚本绕过）
             * @default false
             */
            confirm: boolean;
            /**
             * Key
             * @description 分区键精确匹配；null 不过滤
             */
            key?: string | null;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Offset From
             * @description 起始 offset（含）；null 不限
             */
            offset_from?: number | null;
            /**
             * Offset To
             * @description 结束 offset（不含）；null 不限
             */
            offset_to?: number | null;
            /**
             * Partitions
             * @description 分区号列表（缺省 null = 全部分区；创建时固化为快照，主题扩分区不影响已建任务）
             */
            partitions?: number[] | null;
            /**
             * Rate Per Second
             * @description 限速（条/秒，1~1000）
             * @default 100
             */
            rate_per_second: number;
            /**
             * Time From
             * @description 起始发布时间（含，UTC）；null 不限
             */
            time_from?: string | null;
            /**
             * Time To
             * @description 结束发布时间（不含，UTC）；null 不限
             */
            time_to?: string | null;
            /**
             * Topic
             * @description 主题名（与创建主题时的 name 同值）
             */
            topic: string;
        };
        /**
         * ReplayTaskItem
         * @description 重放任务快照（建任务 / 进度 / 暂停 / 恢复 / 取消 / 列表的响应主体）。
         */
        ReplayTaskItem: {
            /**
             * Affected Groups
             * @description 受影响消费组的影响视图（列表端点为 null，仅详情返回；无订阅组为空数组）
             */
            affected_groups?: components["schemas"]["AffectedGroupItem"][] | null;
            /**
             * Affected Truncated
             * @description 影响基线是否因组数上限被截断（true 表示只展示前若干个组）
             * @default false
             */
            affected_truncated: boolean;
            /**
             * Created At
             * Format: date-time
             * @description 创建时间（UTC）
             */
            created_at: string;
            /**
             * Error Msg
             * @description 失败原因；非 failed 为 null
             */
            error_msg: string | null;
            /**
             * Finished At
             * @description 终态时刻（UTC）；未终结为 null
             */
            finished_at: string | null;
            /**
             * Key
             * @description 分区键精确匹配条件；null 不过滤
             */
            key: string | null;
            /**
             * Log Id First
             * @description 首条成功写入的记录 ID（审计可凭它定位）
             */
            log_id_first: string | null;
            /**
             * Log Id Last
             * @description 末条成功写入的记录 ID（审计可凭它定位）
             */
            log_id_last: string | null;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Offset From
             * @description 起始 offset（含）；null 不限
             */
            offset_from: number | null;
            /**
             * Offset To
             * @description 结束 offset（不含）；null 不限
             */
            offset_to: number | null;
            /**
             * Partitions
             * @description 分区号列表（创建时固化的解析结果）
             */
            partitions: number[];
            /**
             * Rate Per Second
             * @description 限速（条/秒）
             */
            rate_per_second: number;
            /**
             * Replayed
             * @description 已写入条数（进度分子）
             */
            replayed: number;
            /**
             * Status
             * @description 状态（pending / running / paused / cancelled / completed / failed）
             */
            status: string;
            /**
             * Task Id
             * @description 任务 ID（雪花，JSON 输出为字符串）
             */
            task_id: string;
            /**
             * Time From
             * @description 起始发布时间（含，UTC）；null 不限
             */
            time_from: string | null;
            /**
             * Time To
             * @description 结束发布时间（不含，UTC）；null 不限
             */
            time_to: string | null;
            /**
             * Topic
             * @description 主题名（创建时快照，主题停用后仍可展示）
             */
            topic: string;
            /**
             * Total
             * @description 选中总条数（进度分母；建任务时按条件精确计数）
             */
            total: number;
        };
        /**
         * ReplayTaskPage
         * @description 重放任务列表分页（`total` 为 ACL 过滤前的 SQL 计数）。
         */
        ReplayTaskPage: {
            /**
             * Items
             * @description 当页任务（创建时间倒序；已按主题 describe 权限过滤）
             */
            items: components["schemas"]["ReplayTaskItem"][];
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 过滤前总数（ACL 过滤只影响 items，不影响分页基数）
             */
            total: number;
        };
        /**
         * RpcRequest
         * @description RPC 调用请求（JSON 形态）。
         */
        RpcRequest: {
            /**
             * Caller
             * @description 调用方服务标识（非鉴权字段，只透传与记录；与 HTTP 头冲突时以 HTTP 头为准）
             */
            caller?: string | null;
            /**
             * Deadline Ms
             * @description 剩余预算（毫秒，相对量；缺省 30000）。与帧头 `deadline_ms` 同名字段同一语义；服务端扣本跳耗时后把剩余值传给下游，耗尽返回 350004
             */
            deadline_ms?: number | null;
            /**
             * Headers
             * @description 结构化头（透传给处理器并随结果回显）
             */
            headers?: {
                [key: string]: string;
            };
            /**
             * Idempotency Key
             * @description 幂等键（04-4；带键且方法非幂等时服务端按作用域去重，重试全程复用同一键）
             */
            idempotency_key?: string | null;
            /**
             * Payload
             * @description 调用参数（base64 编码；空串表示无参数）
             * @default
             */
            payload: string;
            /**
             * Retry Budget
             * @description 调用链剩余可重试次数（04-4；与帧头 retry_budget 同名同义，逐跳递减防重试风暴）
             */
            retry_budget?: number | null;
            /**
             * Trace Id
             * @description 链路标识（写入日志与响应头；与 PublishRequest 同尺）
             */
            trace_id?: string | null;
        };
        /**
         * RpcResult
         * @description RPC 调用结果（JSON 形态）。
         */
        RpcResult: {
            /**
             * Headers
             * @description 结果结构化头
             */
            headers?: {
                [key: string]: string;
            };
            /**
             * Payload
             * @description 结果（base64 编码；空串表示无结果体）
             */
            payload: string;
        };
        /**
         * TopicCleanupView
         * @description 删主题受理结果（**不是**「已删除」：主题进入 `deleting`，清理异步进行）。
         */
        TopicCleanupView: {
            /** @description 清理进度 */
            cleanup: components["schemas"]["CleanupProgress"];
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Status
             * @description 主题状态（受理后恒为 deleting）
             */
            status: string;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Topic Id
             * @description 主题 ID（雪花，JSON 输出为字符串）
             */
            topic_id: string;
        };
        /**
         * TopicCreateRequest
         * @description 创建主题请求（`tenant_id` **不在请求体**：由基座解析链注入，防客户端自报冒充）。
         */
        TopicCreateRequest: {
            /**
             * Cleanup Policy
             * @description 清理策略（首期只支持 delete）
             * @default delete
             */
            cleanup_policy: string;
            /**
             * Name
             * @description 主题名（建议 {域}.{对象}.{动作}）
             */
            name: string;
            /**
             * Namespace
             * @description 命名空间（对齐 bms module_key；须为已登记且启用的模块）
             */
            namespace: string;
            /**
             * Partition Count
             * @description 分区数（1~1024；只增不减）
             */
            partition_count: number;
            /**
             * Retention Bytes
             * @description 容量上限（字节；0 = 不限，缺省不限）。超限从最老段整段删除
             * @default 0
             */
            retention_bytes: number;
            /**
             * Retention Ms
             * @description 保留时长（毫秒；默认 30 天，上限 366 天）
             * @default 2592000000
             */
            retention_ms: number;
        };
        /**
         * TopicDetail
         * @description 主题详情（含分区列表；`deleting` 时含清理进度）。
         */
        TopicDetail: {
            /** @description 删主题清理进度（仅 deleting 状态有值，其余为 null） */
            cleanup?: components["schemas"]["CleanupProgress"] | null;
            /**
             * Cleanup Policy
             * @description 清理策略
             */
            cleanup_policy: string;
            /**
             * Created At
             * Format: date-time
             * @description 创建时间（UTC）
             */
            created_at: string;
            /**
             * Id
             * @description 主题 ID（雪花，JSON 输出为字符串）
             */
            id: string;
            /**
             * Name
             * @description 主题名
             */
            name: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Partition Count
             * @description 分区数
             */
            partition_count: number;
            /**
             * Partitions
             * @description 分区明细（按分区号升序）
             */
            partitions?: components["schemas"]["bms_msg__schemas__topic__PartitionItem"][];
            /**
             * Retention Bytes
             * @description 容量上限（字节；0 = 不限）
             */
            retention_bytes: number;
            /**
             * Retention Ms
             * @description 保留时长（毫秒）
             */
            retention_ms: number;
            /**
             * Status
             * @description 状态（active / disabled / deleting）
             */
            status: string;
            /**
             * Tenant Id
             * @description 租户内部身份（雪花，JSON 输出为字符串）
             */
            tenant_id: string;
        };
        /**
         * TopicItem
         * @description 主题明细（列表行 / 创建与查询响应的主体）。
         */
        TopicItem: {
            /**
             * Cleanup Policy
             * @description 清理策略
             */
            cleanup_policy: string;
            /**
             * Created At
             * Format: date-time
             * @description 创建时间（UTC）
             */
            created_at: string;
            /**
             * Id
             * @description 主题 ID（雪花，JSON 输出为字符串）
             */
            id: string;
            /**
             * Name
             * @description 主题名
             */
            name: string;
            /**
             * Namespace
             * @description 命名空间
             */
            namespace: string;
            /**
             * Partition Count
             * @description 分区数
             */
            partition_count: number;
            /**
             * Retention Bytes
             * @description 容量上限（字节；0 = 不限）
             */
            retention_bytes: number;
            /**
             * Retention Ms
             * @description 保留时长（毫秒）
             */
            retention_ms: number;
            /**
             * Status
             * @description 状态（active / disabled / deleting）
             */
            status: string;
            /**
             * Tenant Id
             * @description 租户内部身份（雪花，JSON 输出为字符串）
             */
            tenant_id: string;
        };
        /**
         * TopicPage
         * @description 主题列表响应（页码分页：`page` 从 1 起，`size` 默认 20）。
         */
        TopicPage: {
            /**
             * Items
             * @description 当前页主题
             */
            items?: components["schemas"]["TopicItem"][];
            /**
             * Page
             * @description 页码（从 1 起）
             */
            page: number;
            /**
             * Size
             * @description 每页条数
             */
            size: number;
            /**
             * Total
             * @description 符合条件的总数
             */
            total: number;
        };
        /**
         * TopicPartitionExpandRequest
         * @description 扩分区请求（**只增不减**：目标值不大于现状即拒绝，`350009`）。
         */
        TopicPartitionExpandRequest: {
            /**
             * Partition Count
             * @description 目标分区数（须大于当前分区数且在上界内）
             */
            partition_count: number;
        };
        /**
         * TopicRetentionUpdate
         * @description 改保留期请求（两项均可选、至少给一项；范围校验在服务层，越界为 `350009`）。
         */
        TopicRetentionUpdate: {
            /**
             * Retention Bytes
             * @description 容量上限（字节；0 = 不限；null 表示不改）
             */
            retention_bytes?: number | null;
            /**
             * Retention Ms
             * @description 保留时长（毫秒；null 表示不改）
             */
            retention_ms?: number | null;
        };
        /** ValidationError */
        ValidationError: {
            /** Context */
            ctx?: Record<string, never>;
            /** Input */
            input?: unknown;
            /** Location */
            loc: (string | number)[];
            /** Message */
            msg: string;
            /** Error Type */
            type: string;
        };
        /**
         * MessageItem
         * @description 一条消息的头信息与载荷摘要（**不含正文**）。
         */
        bms_msg__schemas__admin_view__MessageItem: {
            /**
             * Headers
             * @description 结构化头（不含载荷）
             */
            headers: {
                [key: string]: string;
            };
            /**
             * Log Id
             * @description 记录 ID（精确检索入口）
             */
            log_id: string;
            /**
             * Message Id
             * @description 消息 ID（重投 / 重放不变）
             */
            message_id: string;
            /**
             * Offset
             * @description 分区内序号
             */
            offset: number;
            /**
             * Parent Span Id
             * @description 生产 span 的父 span
             */
            parent_span_id: string | null;
            /**
             * Partition
             * @description 分区号
             */
            partition: number;
            /**
             * Payload Len
             * @description 落盘载荷字节数
             */
            payload_len: number;
            /**
             * Payload Sha256
             * @description 落盘载荷 SHA-256（64 位小写十六进制）
             */
            payload_sha256: string;
            /**
             * Published At
             * Format: date-time
             * @description 发布时间（UTC）
             */
            published_at: string;
            /**
             * Span Id
             * @description 生产者本次 span 标识
             */
            span_id: string | null;
            /**
             * Trace Flags
             * @description 采样标记（broker 原样透传，不解释）
             */
            trace_flags: string | null;
            /**
             * Trace Id
             * @description 链路标识（06-4；排障时据此串联生产与消费）
             */
            trace_id: string | null;
        };
        /**
         * PartitionItem
         * @description 单个分区的实时运行态（LEO 来自 `MessageStore`，非落库检查点）。
         */
        bms_msg__schemas__admin_view__PartitionItem: {
            /**
             * Committed
             * @description 已提交位点；未指定消费组时为 null
             */
            committed: number | null;
            /**
             * Hw
             * @description 高水位；02-4 交付前恒 null，不伪造单副本数值
             */
            hw: number | null;
            /**
             * Isr
             * @description 同步副本集；02-5 交付前恒空集合
             */
            isr: string[];
            /**
             * Lag
             * @description 积压（leo - committed）；未指定消费组时为 null
             */
            lag: number | null;
            /**
             * Leader
             * @description 当前 leader 实例标识；元数据缺行时为 null
             */
            leader: string | null;
            /**
             * Leo
             * @description 日志末端位点（下一条待分配 offset）
             */
            leo: number;
            /**
             * Owner
             * @description 分区持有者；未指定消费组或组无分配时为 null
             */
            owner: string | null;
            /**
             * Partition
             * @description 分区号（0 起）
             */
            partition: number;
        };
        /**
         * MessageItem
         * @description 一条投递的消息（`delivery_count` 供消费端识别重复；幂等键由业务自选）。
         */
        bms_msg__schemas__message__MessageItem: {
            /**
             * Compression
             * @description 压缩算法（none 表示未压缩，恒有值）
             * @default none
             */
            compression: string;
            /**
             * Content Type
             * @description 消息体内容类型（缺省 application/msgpack，恒有值）
             * @default application/msgpack
             */
            content_type: string;
            /**
             * Delivery Count
             * @description 投递次数（首次 1，重投递增）
             */
            delivery_count: number;
            /**
             * Headers
             * @description 结构化头
             */
            headers?: {
                [key: string]: string;
            };
            /**
             * Key
             * @description 分区键
             */
            key: string | null;
            /**
             * Message Id
             * @description 消息 ID（重投保持不变）
             */
            message_id: string;
            /**
             * Offset
             * @description 分区内序号
             */
            offset: number;
            /**
             * Parent Span Id
             * @description 生产 span 的父 span（链路起点可为空）
             */
            parent_span_id: string | null;
            /**
             * Partition
             * @description 分区号
             */
            partition: number;
            /**
             * Payload
             * @description 消息体（base64 编码；帧路径由私有原始字节通道替代）
             */
            payload: string;
            /**
             * Published At
             * Format: date-time
             * @description 发布时间（UTC）
             */
            published_at: string;
            /**
             * Span Id
             * @description 生产者本次 span 标识（消费端据它建子 span）
             */
            span_id: string | null;
            /**
             * Topic
             * @description 主题名
             */
            topic: string;
            /**
             * Trace Flags
             * @description 采样标记（约定 W3C 8 位十六进制；broker 不解释，原样透传）
             */
            trace_flags: string | null;
            /**
             * Trace Id
             * @description 链路标识
             */
            trace_id: string | null;
            /**
             * Uncompressed Len
             * @description 解压后字节数（生产者可选声明）
             */
            uncompressed_len?: number | null;
        };
        /**
         * PartitionItem
         * @description 分区明细（详情接口的 `partitions` 元素）。
         */
        bms_msg__schemas__topic__PartitionItem: {
            /**
             * Checkpoint Hw
             * @description 检查点 HW
             */
            checkpoint_hw: number;
            /**
             * Checkpoint Leo
             * @description 检查点 LEO
             */
            checkpoint_leo: number;
            /**
             * Epoch
             * @description 领导者任期（复制阶段使用）
             */
            epoch: number;
            /**
             * Leader
             * @description 当前 leader 标识
             */
            leader: string;
            /**
             * Partition No
             * @description 分区号（0 起）
             */
            partition_no: number;
        };
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
export interface operations {
    root__get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    ack_api_v1_msg_ack_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AckRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_AckResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_acls_api_v1_msg_admin_acls_get: {
        parameters: {
            query?: {
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 主体类型过滤（user / role） */
                principal_type?: string | null;
                /** @description 主体标识过滤 */
                principal_key?: string | null;
                /** @description 动作过滤 */
                action?: string | null;
                /** @description 状态过滤（enabled / disabled） */
                status?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_AclPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    upsert_acls_api_v1_msg_admin_acls_put: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AclBatchRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_AclBatchResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    revoke_acl_api_v1_msg_admin_acls__acl_id__delete: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description ACL 规则 ID（雪花） */
                acl_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_AclRevokeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_delayed_api_v1_msg_admin_delayed_get: {
        parameters: {
            query?: {
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 分区号过滤 */
                partition?: number | null;
                /** @description 延迟档位过滤（1s / 5s / … / 60m） */
                level?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DelayedPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_delay_summary_api_v1_msg_admin_delayed_summary_get: {
        parameters: {
            query?: {
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 分区号过滤 */
                partition?: number | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DelaySummary_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_dead_letters_api_v1_msg_admin_dlq_get: {
        parameters: {
            query?: {
                /** @description 状态过滤；缺省排除 archived */
                status?: string | null;
                /** @description 转入原因过滤 */
                reason?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 分区号过滤 */
                partition?: number | null;
                /** @description 失败时间下限（含，UTC） */
                failed_from?: string | null;
                /** @description 失败时间上限（不含，UTC） */
                failed_to?: string | null;
                /** @description log_id / message_id 模糊匹配 */
                keyword?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DlqList_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_dead_letter_api_v1_msg_admin_dlq__dlq_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 死信 ID（雪花） */
                dlq_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DlqDetail_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    archive_dead_letter_api_v1_msg_admin_dlq__dlq_id__archive_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 死信 ID（雪花） */
                dlq_id: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["DlqArchiveRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DlqDisposeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    ignore_dead_letter_api_v1_msg_admin_dlq__dlq_id__ignore_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 死信 ID（雪花） */
                dlq_id: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["DlqIgnoreRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DlqDisposeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    replay_dead_letter_api_v1_msg_admin_dlq__dlq_id__replay_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 死信 ID（雪花） */
                dlq_id: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["DlqReplayRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DlqDisposeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_groups_api_v1_msg_admin_groups_get: {
        parameters: {
            query?: {
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_GroupPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_group_api_v1_msg_admin_groups__namespace___topic___group__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 命名空间 */
                namespace: string;
                /** @description 主题名 */
                topic: string;
                /** @description 消费组名 */
                group: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_GroupDetail_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    search_messages_api_v1_msg_admin_messages_get: {
        parameters: {
            query: {
                /** @description 命名空间 */
                namespace: string;
                /** @description 主题名 */
                topic: string;
                /** @description 分区号过滤（缺省全部） */
                partition?: number | null;
                /** @description 起始 offset（含） */
                offset_from?: number | null;
                /** @description 结束 offset（不含） */
                offset_to?: number | null;
                /** @description 分区键精确值 */
                key?: string | null;
                /** @description 发布时间下界（ISO 8601，含） */
                time_from?: string | null;
                /** @description 发布时间上界（ISO 8601，不含） */
                time_to?: string | null;
                /** @description 记录 ID（精确命中走索引） */
                log_id?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数（1~100） */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_MessagePage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    reset_offset_api_v1_msg_admin_offsets_reset_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["OffsetResetRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_OffsetResetResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_overview_api_v1_msg_admin_overview_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_Overview_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_partitions_api_v1_msg_admin_partitions_get: {
        parameters: {
            query: {
                /** @description 命名空间 */
                namespace: string;
                /** @description 主题名 */
                topic: string;
                /** @description 分区号过滤（缺省全部） */
                partition?: number | null;
                /** @description 消费组（缺省不给已提交位点） */
                group?: string | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_PartitionPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_quotas_api_v1_msg_admin_quotas_get: {
        parameters: {
            query?: {
                /** @description 作用域（空串 = 租户级） */
                namespace?: string;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_QuotaView_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    update_quotas_api_v1_msg_admin_quotas_put: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["QuotaUpdateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_QuotaView_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_replay_api_v1_msg_admin_replay_get: {
        parameters: {
            query?: {
                /** @description 状态过滤（六态之一） */
                status?: string | null;
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名过滤 */
                topic?: string | null;
                /** @description 创建时间下限（含，UTC） */
                created_from?: string | null;
                /** @description 创建时间上限（不含，UTC） */
                created_to?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    create_replay_api_v1_msg_admin_replay_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ReplayRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_replay_api_v1_msg_admin_replay__task_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 重放任务 ID（雪花） */
                task_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    cancel_replay_api_v1_msg_admin_replay__task_id__cancel_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 重放任务 ID（雪花） */
                task_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    pause_replay_api_v1_msg_admin_replay__task_id__pause_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 重放任务 ID（雪花） */
                task_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    resume_replay_api_v1_msg_admin_replay__task_id__resume_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 重放任务 ID（雪花） */
                task_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ReplayTaskItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    list_topics_api_v1_msg_admin_topics_get: {
        parameters: {
            query?: {
                /** @description 命名空间过滤 */
                namespace?: string | null;
                /** @description 主题名模糊匹配 */
                keyword?: string | null;
                /** @description 页码（从 1 起） */
                page?: number;
                /** @description 每页条数 */
                size?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicPage_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    create_topic_api_v1_msg_admin_topics_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TopicCreateRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    get_topic_api_v1_msg_admin_topics__topic_id__get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 主题 ID（雪花） */
                topic_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicDetail_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    delete_topic_api_v1_msg_admin_topics__topic_id__delete: {
        parameters: {
            query?: {
                /** @description 显式确认（必须为 true；缺省 / false 一律拒绝） */
                confirm?: boolean;
            };
            header?: never;
            path: {
                /** @description 主题 ID（雪花） */
                topic_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicCleanupView_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    update_topic_api_v1_msg_admin_topics__topic_id__patch: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 主题 ID（雪花） */
                topic_id: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TopicRetentionUpdate"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicItem_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    topic_delete_impact_api_v1_msg_admin_topics__topic_id__delete_impact_get: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 主题 ID（雪花） */
                topic_id: number;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_DeleteImpact_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    expand_topic_partitions_api_v1_msg_admin_topics__topic_id__partitions_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 主题 ID（雪花） */
                topic_id: number;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["TopicPartitionExpandRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_TopicDetail_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    consume_api_v1_msg_consume_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ConsumeRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ConsumeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    heartbeat_api_v1_msg_heartbeat_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["MemberRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_MemberAssignment_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    join_api_v1_msg_join_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["MemberRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_MemberAssignment_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    leave_api_v1_msg_leave_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["MemberRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_LeaveResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    nack_api_v1_msg_nack_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["NackRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_NackResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    publish_api_v1_msg_publish_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PublishRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_PublishResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    tenant_usage_api_v1_msg_tenant_usage_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    quota_projection_api_v1_msg_tenant_usage_quota_get: {
        parameters: {
            query?: {
                /** @description 平台投影读取器传入的租户主键（缺省取上下文） */
                tenant_id?: number | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    quota_projection_bytes_api_v1_msg_tenant_usage_quota_bytes_get: {
        parameters: {
            query?: {
                /** @description 平台投影读取器传入的租户主键（缺省取上下文） */
                tenant_id?: number | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    quota_projection_rate_api_v1_msg_tenant_usage_quota_rate_get: {
        parameters: {
            query?: {
                /** @description 平台投影读取器传入的租户主键（缺省取上下文） */
                tenant_id?: number | null;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    healthz_healthz_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": {
                        [key: string]: unknown;
                    };
                };
            };
        };
    };
    ack_msg_ack_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["AckRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_AckResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    connection_msg_connection_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ConnectionInfo_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    heartbeat_msg_connection_heartbeat_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    consume_msg_consume_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["ConsumeRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_ConsumeResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    nack_msg_nack_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["NackRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_NackResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    publish_msg_publish_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["PublishRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_PublishResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    subscribe_msg_subscribe_get: {
        parameters: {
            query: {
                /** @description 命名空间（对齐 bms module_key；须为已登记且启用的模块） */
                namespace: string;
                /** @description 主题名（与创建主题时的 name 同值） */
                topic: string;
                /** @description 消费组名（组元数据与再均衡归 01-4） */
                group: string;
                /** @description 成员标识（客户端自带，组内唯一；同 id 再次 join 视为重连） */
                consumer_id: string;
                /** @description 分区号（必带；组内分摊与再均衡归 01-4） */
                partition: number;
                /** @description 单批拉取条数（与 consume 同口径） */
                max_messages?: number;
                /** @description 单条流时长上限（秒）；到期以 close(deadline) 正常结束，由客户端重连 */
                max_seconds?: number;
            };
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description SSE 长驻流（event: open / message / error / close） */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "text/event-stream": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    subscribe_frames_msg_subscribe_post: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description 帧流（open 帧 4+RESPONSE / message 帧 6 / error 帧 16；正文 EOF 即正常结束） */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/x-msg-frame": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    readyz_readyz_get: {
        parameters: {
            query?: never;
            header?: never;
            path?: never;
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": unknown;
                };
            };
        };
    };
    invoke_rpc__service___method__post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 服务标识（与注册表登记的 service 同值） */
                service: string;
                /** @description 方法名（与注册表登记的 method 同值） */
                method: string;
            };
            cookie?: never;
        };
        requestBody: {
            content: {
                "application/json": components["schemas"]["RpcRequest"];
            };
        };
        responses: {
            /** @description Successful Response */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse_RpcResult_"];
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
    open_stream_rpc__service___method__stream_post: {
        parameters: {
            query?: never;
            header?: never;
            path: {
                /** @description 服务标识（与注册表登记的 service 同值） */
                service: string;
                /** @description 流式方法名（与注册表登记的 method 同值） */
                method: string;
            };
            cookie?: never;
        };
        requestBody?: never;
        responses: {
            /** @description 帧流（48+response 逐帧、末帧另置 end_of_stream；ERROR 帧 16 收尾；EOF 即结束） */
            200: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/x-msg-frame": unknown;
                };
            };
            /** @description 未认证 */
            401: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 无权限 */
            403: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 资源不存在 */
            404: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description Validation Error */
            422: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["HTTPValidationError"];
                };
            };
            /** @description 限流 */
            429: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
            /** @description 服务异常 */
            500: {
                headers: {
                    [name: string]: unknown;
                };
                content: {
                    "application/json": components["schemas"]["ApiResponse"];
                };
            };
        };
    };
}
