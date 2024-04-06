use super::*;

const INTERNAL_PING: &str = "ruserf-ping";
const INTERNAL_CONFLICT: &str = "ruserf-conflict";
#[cfg(feature = "encryption")]
pub(crate) const INTERNAL_INSTALL_KEY: &str = "ruserf-install-key";
#[cfg(feature = "encryption")]
pub(crate) const INTERNAL_USE_KEY: &str = "ruserf-use-key";
#[cfg(feature = "encryption")]
pub(crate) const INTERNAL_REMOVE_KEY: &str = "ruserf-remove-key";
#[cfg(feature = "encryption")]
pub(crate) const INTERNAL_LIST_KEYS: &str = "ruserf-list-keys";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum CrateEventType {
  Member(MemberEventType),
  User,
  Query,
  InternalQuery,
}

pub(crate) enum CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  Member(MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  User(UserEventMessage),
  Query(QueryEvent<T, D>),
  InternalQuery {
    kind: InternalQueryEvent<T::Id>,
    query: QueryEvent<T, D>,
  },
}

impl<D, T> Clone for CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn clone(&self) -> Self {
    match self {
      Self::Member(e) => Self::Member(e.clone()),
      Self::User(e) => Self::User(e.clone()),
      Self::Query(e) => Self::Query(e.clone()),
      Self::InternalQuery { kind, query } => Self::InternalQuery {
        kind: kind.clone(),
        query: query.clone(),
      },
    }
  }
}

impl<D, T> CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  /// Returns the type of the event
  #[cfg(feature = "test")]
  #[inline]
  pub(crate) fn ty(&self) -> CrateEventType {
    match self {
      Self::Member(e) => CrateEventType::Member(e.ty),
      Self::User(_) => CrateEventType::User,
      Self::Query(_) => CrateEventType::Query,
      Self::InternalQuery { .. } => CrateEventType::InternalQuery,
    }
  }

  pub(crate) fn is_internal_query(&self) -> bool {
    matches!(self, Self::InternalQuery { .. })
  }
}

impl<D, T> From<MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
  for CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>) -> Self {
    Self::Member(value)
  }
}

impl<D, T> From<UserEventMessage> for CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: UserEventMessage) -> Self {
    Self::User(value)
  }
}

impl<D, T> From<QueryEvent<T, D>> for CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: QueryEvent<T, D>) -> Self {
    Self::Query(value)
  }
}

impl<D, T> From<(InternalQueryEvent<T::Id>, QueryEvent<T, D>)> for CrateEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: (InternalQueryEvent<T::Id>, QueryEvent<T, D>)) -> Self {
    Self::InternalQuery {
      kind: value.0,
      query: value.1,
    }
  }
}

pub enum InternalQueryEvent<I> {
  Ping,
  Conflict(I),
  #[cfg(feature = "encryption")]
  InstallKey,
  #[cfg(feature = "encryption")]
  UseKey,
  #[cfg(feature = "encryption")]
  RemoveKey,
  #[cfg(feature = "encryption")]
  ListKey,
}

impl<I: Clone> Clone for InternalQueryEvent<I> {
  fn clone(&self) -> Self {
    match self {
      Self::Ping => Self::Ping,
      Self::Conflict(e) => Self::Conflict(e.clone()),
      #[cfg(feature = "encryption")]
      Self::InstallKey => Self::InstallKey,
      #[cfg(feature = "encryption")]
      Self::UseKey => Self::UseKey,
      #[cfg(feature = "encryption")]
      Self::RemoveKey => Self::RemoveKey,
      #[cfg(feature = "encryption")]
      Self::ListKey => Self::ListKey,
    }
  }
}

impl<I> InternalQueryEvent<I> {
  #[inline]
  pub(crate) const fn as_str(&self) -> &'static str {
    match self {
      Self::Ping => INTERNAL_PING,
      Self::Conflict(_) => INTERNAL_CONFLICT,
      #[cfg(feature = "encryption")]
      Self::InstallKey => INTERNAL_INSTALL_KEY,
      #[cfg(feature = "encryption")]
      Self::UseKey => INTERNAL_USE_KEY,
      #[cfg(feature = "encryption")]
      Self::RemoveKey => INTERNAL_REMOVE_KEY,
      #[cfg(feature = "encryption")]
      Self::ListKey => INTERNAL_LIST_KEYS,
    }
  }
}
