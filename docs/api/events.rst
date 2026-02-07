Events API
==========

.. module:: stabilize.events

Configuration
-------------

.. autofunction:: configure_event_sourcing

.. autofunction:: configure_event_bus

.. autofunction:: configure_event_recorder

Base Types
----------

.. autoclass:: Event
   :members:

.. autoclass:: EventType
   :members:
   :undoc-members:

.. autoclass:: EntityType
   :members:
   :undoc-members:

.. autoclass:: EventMetadata
   :members:

Event Bus
---------

.. autoclass:: EventBus
   :members:

.. autoclass:: Subscription
   :members:

.. autoclass:: SubscriptionMode
   :members:
   :undoc-members:

.. autoclass:: EventBusStats
   :members:

.. autofunction:: get_event_bus

.. autofunction:: reset_event_bus

Event Recorder
--------------

.. autoclass:: EventRecorder
   :members:

.. autofunction:: get_event_recorder

.. autofunction:: set_event_context

.. autofunction:: get_event_metadata

Event Stores
------------

.. autoclass:: EventStore
   :members:

.. autoclass:: EventQuery
   :members:

.. autoclass:: AppendResult
   :members:

.. autoclass:: InMemoryEventStore
   :members:

.. autoclass:: SqliteEventStore
   :members:

Projections
-----------

.. autoclass:: Projection
   :members:

.. autoclass:: StageMetricsProjection
   :members:

.. autoclass:: StageMetrics
   :members:

.. autoclass:: WorkflowTimelineProjection
   :members:

.. autoclass:: WorkflowTimeline
   :members:

.. autoclass:: TimelineEntry
   :members:

Replay
------

.. autoclass:: EventReplayer
   :members:

.. autoclass:: ReplayResult
   :members:

.. autoclass:: WorkflowState
   :members:

Snapshots
---------

.. autoclass:: Snapshot
   :members:

.. autoclass:: SnapshotStore
   :members:

.. autoclass:: SnapshotPolicy
   :members:

Subscriptions
-------------

.. autoclass:: DurableSubscription
   :members:

.. autoclass:: SubscriptionManager
   :members:
